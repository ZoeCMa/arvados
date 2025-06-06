// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0
//
//
// How to manually run individual tests against the real cloud:
//
// $ go test -v git.arvados.org/arvados.git/lib/cloud/ec2 -live-ec2-cfg ec2config.yml -check.f=TestCreate
//
// Tests should be run individually and in the order they are listed in the file:
//
// Example ec2config.yml:
//
// ImageIDForTestSuite: ami-xxxxxxxxxxxxxxxxx
// DriverParameters:
//       AccessKeyID: XXXXXXXXXXXXXX
//       SecretAccessKey: xxxxxxxxxxxxxxxxxxxx
//       Region: us-east-1
//       SecurityGroupIDs: [sg-xxxxxxxx]
//       SubnetID: subnet-xxxxxxxx
//       AdminUsername: crunch

package ec2

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os/exec"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"git.arvados.org/arvados.git/lib/cloud"
	libconfig "git.arvados.org/arvados.git/lib/config"
	"git.arvados.org/arvados.git/lib/dispatchcloud/test"
	"git.arvados.org/arvados.git/sdk/go/arvados"
	"git.arvados.org/arvados.git/sdk/go/arvadostest"
	"git.arvados.org/arvados.git/sdk/go/config"
	"git.arvados.org/arvados.git/sdk/go/ctxlog"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/ghodss/yaml"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	check "gopkg.in/check.v1"
)

var live = flag.String("live-ec2-cfg", "", "Test with real EC2 API, provide config file")

// Gocheck boilerplate
func Test(t *testing.T) {
	check.TestingT(t)
}

type sliceOrStringSuite struct{}

var _ = check.Suite(&sliceOrStringSuite{})

func (s *sliceOrStringSuite) TestUnmarshal(c *check.C) {
	var conf ec2InstanceSetConfig
	for _, trial := range []struct {
		input  string
		output sliceOrSingleString
	}{
		{``, nil},
		{`""`, nil},
		{`[]`, nil},
		{`"foo"`, sliceOrSingleString{"foo"}},
		{`["foo"]`, sliceOrSingleString{"foo"}},
		{`[foo]`, sliceOrSingleString{"foo"}},
		{`["foo", "bar"]`, sliceOrSingleString{"foo", "bar"}},
		{`[foo-bar, baz]`, sliceOrSingleString{"foo-bar", "baz"}},
	} {
		c.Logf("trial: %+v", trial)
		err := yaml.Unmarshal([]byte("SubnetID: "+trial.input+"\n"), &conf)
		if !c.Check(err, check.IsNil) {
			continue
		}
		c.Check(conf.SubnetID, check.DeepEquals, trial.output)
	}
}

type EC2InstanceSetSuite struct{}

var _ = check.Suite(&EC2InstanceSetSuite{})

type testConfig struct {
	ImageIDForTestSuite string
	DriverParameters    json.RawMessage
}

type ec2stub struct {
	c                     *check.C
	reftime               time.Time
	importKeyPairCalls    []*ec2.ImportKeyPairInput
	describeKeyPairsCalls []*ec2.DescribeKeyPairsInput
	runInstancesCalls     []*ec2.RunInstancesInput
	// {subnetID => error}: RunInstances returns error if subnetID
	// matches.
	subnetErrorOnRunInstances map[string]error
}

func (e *ec2stub) ImportKeyPair(ctx context.Context, input *ec2.ImportKeyPairInput, _ ...func(*ec2.Options)) (*ec2.ImportKeyPairOutput, error) {
	e.importKeyPairCalls = append(e.importKeyPairCalls, input)
	return nil, nil
}

func (e *ec2stub) DescribeKeyPairs(ctx context.Context, input *ec2.DescribeKeyPairsInput, _ ...func(*ec2.Options)) (*ec2.DescribeKeyPairsOutput, error) {
	e.describeKeyPairsCalls = append(e.describeKeyPairsCalls, input)
	return &ec2.DescribeKeyPairsOutput{}, nil
}

func (e *ec2stub) RunInstances(ctx context.Context, input *ec2.RunInstancesInput, _ ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error) {
	e.runInstancesCalls = append(e.runInstancesCalls, input)
	if len(input.NetworkInterfaces) > 0 && input.NetworkInterfaces[0].SubnetId != nil {
		err := e.subnetErrorOnRunInstances[*input.NetworkInterfaces[0].SubnetId]
		if err != nil {
			return nil, err
		}
	}
	return &ec2.RunInstancesOutput{Instances: []types.Instance{{
		InstanceId:   aws.String("i-123"),
		InstanceType: types.InstanceTypeT2Micro,
		Tags:         input.TagSpecifications[0].Tags,
	}}}, nil
}

func (e *ec2stub) DescribeInstances(ctx context.Context, input *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	return &ec2.DescribeInstancesOutput{
		Reservations: []types.Reservation{{
			Instances: []types.Instance{{
				InstanceId:        aws.String("i-123"),
				InstanceLifecycle: types.InstanceLifecycleTypeSpot,
				InstanceType:      types.InstanceTypeT2Micro,
				PrivateIpAddress:  aws.String("10.1.2.3"),
				State:             &types.InstanceState{Name: types.InstanceStateNameRunning, Code: aws.Int32(16)},
			}, {
				InstanceId:        aws.String("i-124"),
				InstanceLifecycle: types.InstanceLifecycleTypeSpot,
				InstanceType:      types.InstanceTypeT2Micro,
				PrivateIpAddress:  aws.String("10.1.2.4"),
				State:             &types.InstanceState{Name: types.InstanceStateNameRunning, Code: aws.Int32(16)},
			}},
		}},
	}, nil
}

func (e *ec2stub) DescribeInstanceStatus(ctx context.Context, input *ec2.DescribeInstanceStatusInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceStatusOutput, error) {
	return &ec2.DescribeInstanceStatusOutput{
		InstanceStatuses: []types.InstanceStatus{{
			InstanceId:       aws.String("i-123"),
			AvailabilityZone: aws.String("aa-east-1a"),
		}, {
			InstanceId:       aws.String("i-124"),
			AvailabilityZone: aws.String("aa-east-1a"),
		}},
	}, nil
}

func (e *ec2stub) DescribeSpotPriceHistory(ctx context.Context, input *ec2.DescribeSpotPriceHistoryInput, _ ...func(*ec2.Options)) (*ec2.DescribeSpotPriceHistoryOutput, error) {
	if input.NextToken == nil || *input.NextToken == "" {
		return &ec2.DescribeSpotPriceHistoryOutput{
			SpotPriceHistory: []types.SpotPrice{
				types.SpotPrice{
					InstanceType:     types.InstanceTypeT2Micro,
					AvailabilityZone: aws.String("aa-east-1a"),
					SpotPrice:        aws.String("0.005"),
					Timestamp:        aws.Time(e.reftime.Add(-9 * time.Minute)),
				},
				types.SpotPrice{
					InstanceType:     types.InstanceTypeT2Micro,
					AvailabilityZone: aws.String("aa-east-1a"),
					SpotPrice:        aws.String("0.015"),
					Timestamp:        aws.Time(e.reftime.Add(-5 * time.Minute)),
				},
			},
			NextToken: aws.String("stubnexttoken"),
		}, nil
	} else {
		return &ec2.DescribeSpotPriceHistoryOutput{
			SpotPriceHistory: []types.SpotPrice{
				types.SpotPrice{
					InstanceType:     types.InstanceTypeT2Micro,
					AvailabilityZone: aws.String("aa-east-1a"),
					SpotPrice:        aws.String("0.01"),
					Timestamp:        aws.Time(e.reftime.Add(-2 * time.Minute)),
				},
			},
			NextToken: aws.String(""), // see bug #22400
		}, nil
	}
}

func (e *ec2stub) CreateTags(ctx context.Context, input *ec2.CreateTagsInput, _ ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error) {
	return nil, nil
}

func (e *ec2stub) TerminateInstances(ctx context.Context, input *ec2.TerminateInstancesInput, _ ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error) {
	return nil, nil
}

type ec2stubError = smithy.GenericAPIError

// Ensure ec2stubError satisfies the smithy.APIError interface
var _ = smithy.APIError(&ec2stubError{})

func GetInstanceSet(c *check.C, conf string) (*ec2InstanceSet, cloud.ImageID, arvados.Cluster, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	cluster := arvados.Cluster{
		InstanceTypes: arvados.InstanceTypeMap(map[string]arvados.InstanceType{
			"tiny": {
				Name:         "tiny",
				ProviderType: "t2.micro",
				VCPUs:        1,
				RAM:          4000000000,
				Scratch:      10000000000,
				Price:        .02,
				Preemptible:  false,
			},
			"tiny-with-extra-scratch": {
				Name:         "tiny-with-extra-scratch",
				ProviderType: "t2.micro",
				VCPUs:        1,
				RAM:          4000000000,
				Price:        .02,
				Preemptible:  false,
				AddedScratch: 20000000000,
			},
			"tiny-preemptible": {
				Name:         "tiny-preemptible",
				ProviderType: "t2.micro",
				VCPUs:        1,
				RAM:          4000000000,
				Scratch:      10000000000,
				Price:        .02,
				Preemptible:  true,
			},
		})}
	if *live != "" {
		var exampleCfg testConfig
		err := config.LoadFile(&exampleCfg, *live)
		c.Assert(err, check.IsNil)

		is, err := newEC2InstanceSet(exampleCfg.DriverParameters, "test123", nil, logrus.StandardLogger(), reg)
		c.Assert(err, check.IsNil)
		return is.(*ec2InstanceSet), cloud.ImageID(exampleCfg.ImageIDForTestSuite), cluster, reg
	} else {
		is, err := newEC2InstanceSet(json.RawMessage(conf), "test123", nil, ctxlog.TestLogger(c), reg)
		c.Assert(err, check.IsNil)
		is.(*ec2InstanceSet).client = &ec2stub{c: c, reftime: time.Now().UTC()}
		return is.(*ec2InstanceSet), cloud.ImageID("blob"), cluster, reg
	}
}

func (*EC2InstanceSetSuite) TestCreate(c *check.C) {
	ap, img, cluster, _ := GetInstanceSet(c, "{}")
	pk, _ := test.LoadTestKey(c, "../../dispatchcloud/test/sshkey_dispatch")

	inst, err := ap.Create(cluster.InstanceTypes["tiny"],
		img, map[string]string{
			"TestTagName": "test tag value",
		}, "umask 0600; echo -n test-file-data >/var/run/test-file", pk)
	c.Assert(err, check.IsNil)

	tags := inst.Tags()
	c.Check(tags["TestTagName"], check.Equals, "test tag value")
	c.Logf("inst.String()=%v Address()=%v Tags()=%v", inst.String(), inst.Address(), tags)

	if *live == "" {
		c.Check(ap.client.(*ec2stub).describeKeyPairsCalls, check.HasLen, 1)
		c.Check(ap.client.(*ec2stub).importKeyPairCalls, check.HasLen, 1)

		runcalls := ap.client.(*ec2stub).runInstancesCalls
		if c.Check(runcalls, check.HasLen, 1) {
			c.Check(runcalls[0].MetadataOptions.HttpEndpoint, check.DeepEquals, types.InstanceMetadataEndpointStateEnabled)
			c.Check(runcalls[0].MetadataOptions.HttpTokens, check.DeepEquals, types.HttpTokensStateRequired)
		}
	}
}

func (*EC2InstanceSetSuite) TestCreateWithExtraScratch(c *check.C) {
	ap, img, cluster, _ := GetInstanceSet(c, "{}")
	inst, err := ap.Create(cluster.InstanceTypes["tiny-with-extra-scratch"],
		img, map[string]string{
			"TestTagName": "test tag value",
		}, "umask 0600; echo -n test-file-data >/var/run/test-file", nil)

	c.Assert(err, check.IsNil)

	tags := inst.Tags()
	c.Check(tags["TestTagName"], check.Equals, "test tag value")
	c.Logf("inst.String()=%v Address()=%v Tags()=%v", inst.String(), inst.Address(), tags)

	if *live == "" {
		// Should not have called key pair APIs, because
		// publickey arg was nil
		c.Check(ap.client.(*ec2stub).describeKeyPairsCalls, check.HasLen, 0)
		c.Check(ap.client.(*ec2stub).importKeyPairCalls, check.HasLen, 0)
	}
}

func (*EC2InstanceSetSuite) TestCreatePreemptible(c *check.C) {
	ap, img, cluster, _ := GetInstanceSet(c, "{}")
	pk, _ := test.LoadTestKey(c, "../../dispatchcloud/test/sshkey_dispatch")

	inst, err := ap.Create(cluster.InstanceTypes["tiny-preemptible"],
		img, map[string]string{
			"TestTagName": "test tag value",
		}, "umask 0600; echo -n test-file-data >/var/run/test-file", pk)

	c.Assert(err, check.IsNil)

	tags := inst.Tags()
	c.Check(tags["TestTagName"], check.Equals, "test tag value")
	c.Logf("inst.String()=%v Address()=%v Tags()=%v", inst.String(), inst.Address(), tags)

}

func (*EC2InstanceSetSuite) TestCreateFailoverSecondSubnet(c *check.C) {
	if *live != "" {
		c.Skip("not applicable in live mode")
		return
	}

	ap, img, cluster, reg := GetInstanceSet(c, `{"SubnetID":["subnet-full","subnet-good"]}`)
	ap.client.(*ec2stub).subnetErrorOnRunInstances = map[string]error{
		"subnet-full": &ec2stubError{
			Code:    "InsufficientFreeAddressesInSubnet",
			Message: "subnet is full",
		},
	}
	inst, err := ap.Create(cluster.InstanceTypes["tiny"], img, nil, "", nil)
	c.Check(err, check.IsNil)
	c.Check(inst, check.NotNil)
	c.Check(ap.client.(*ec2stub).runInstancesCalls, check.HasLen, 2)
	metrics := arvadostest.GatherMetricsAsString(reg)
	c.Check(metrics, check.Matches, `(?ms).*`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-full",success="0"} 1\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-full",success="1"} 0\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-good",success="0"} 0\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-good",success="1"} 1\n`+
		`.*`)

	// Next RunInstances call should try the working subnet first
	inst, err = ap.Create(cluster.InstanceTypes["tiny"], img, nil, "", nil)
	c.Check(err, check.IsNil)
	c.Check(inst, check.NotNil)
	c.Check(ap.client.(*ec2stub).runInstancesCalls, check.HasLen, 3)
	metrics = arvadostest.GatherMetricsAsString(reg)
	c.Check(metrics, check.Matches, `(?ms).*`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-full",success="0"} 1\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-full",success="1"} 0\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-good",success="0"} 0\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-good",success="1"} 2\n`+
		`.*`)
}

func (*EC2InstanceSetSuite) TestIsErrorSubnetSpecific(c *check.C) {
	c.Check(isErrorSubnetSpecific(nil), check.Equals, false)
	c.Check(isErrorSubnetSpecific(errors.New("misc error")), check.Equals, false)

	c.Check(isErrorSubnetSpecific(&ec2stubError{
		Code: "InsufficientInstanceCapacity",
	}), check.Equals, true)

	c.Check(isErrorSubnetSpecific(&ec2stubError{
		Code: "InsufficientVolumeCapacity",
	}), check.Equals, true)

	c.Check(isErrorSubnetSpecific(&ec2stubError{
		Code:    "InsufficientFreeAddressesInSubnet",
		Message: "Not enough free addresses in subnet subnet-abcdefg\n\tstatus code: 400, request id: abcdef01-2345-6789-abcd-ef0123456789",
	}), check.Equals, true)

	// #21603: (Sometimes?) EC2 returns code InvalidParameterValue
	// even though the code "InsufficientFreeAddressesInSubnet"
	// seems like it must be meant for exactly this error.
	c.Check(isErrorSubnetSpecific(&ec2stubError{
		Code:    "InvalidParameterValue",
		Message: "Not enough free addresses in subnet subnet-abcdefg\n\tstatus code: 400, request id: abcdef01-2345-6789-abcd-ef0123456789",
	}), check.Equals, true)

	// Similarly, AWS docs
	// (https://repost.aws/knowledge-center/vpc-insufficient-ip-errors)
	// suggest the following code/message combinations also exist.
	c.Check(isErrorSubnetSpecific(&ec2stubError{
		Code:    "Client.InvalidParameterValue",
		Message: "There aren't sufficient free Ipv4 addresses or prefixes",
	}), check.Equals, true)
	c.Check(isErrorSubnetSpecific(&ec2stubError{
		Code:    "InvalidParameterValue",
		Message: "There aren't sufficient free Ipv4 addresses or prefixes",
	}), check.Equals, true)
	// Meanwhile, other AWS docs
	// (https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html)
	// suggest Client.InvalidParameterValue is not a real code but
	// ClientInvalidParameterValue is.
	c.Check(isErrorSubnetSpecific(&ec2stubError{
		Code:    "ClientInvalidParameterValue",
		Message: "There aren't sufficient free Ipv4 addresses or prefixes",
	}), check.Equals, true)

	c.Check(isErrorSubnetSpecific(&ec2stubError{
		Code:    "InvalidParameterValue",
		Message: "Some other invalid parameter error",
	}), check.Equals, false)
}

func (*EC2InstanceSetSuite) TestCreateAllSubnetsFailing(c *check.C) {
	if *live != "" {
		c.Skip("not applicable in live mode")
		return
	}

	ap, img, cluster, reg := GetInstanceSet(c, `{"SubnetID":["subnet-full","subnet-broken"]}`)
	ap.client.(*ec2stub).subnetErrorOnRunInstances = map[string]error{
		"subnet-full": &ec2stubError{
			Code:    "InsufficientFreeAddressesInSubnet",
			Message: "subnet is full",
		},
		"subnet-broken": &ec2stubError{
			Code:    "InvalidSubnetId.NotFound",
			Message: "bogus subnet id",
		},
	}
	_, err := ap.Create(cluster.InstanceTypes["tiny"], img, nil, "", nil)
	c.Check(err, check.NotNil)
	c.Check(err, check.ErrorMatches, `.*InvalidSubnetId\.NotFound.*`)
	c.Check(ap.client.(*ec2stub).runInstancesCalls, check.HasLen, 2)
	metrics := arvadostest.GatherMetricsAsString(reg)
	c.Check(metrics, check.Matches, `(?ms).*`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-broken",success="0"} 1\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-broken",success="1"} 0\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-full",success="0"} 1\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-full",success="1"} 0\n`+
		`.*`)

	_, err = ap.Create(cluster.InstanceTypes["tiny"], img, nil, "", nil)
	c.Check(err, check.NotNil)
	c.Check(err, check.ErrorMatches, `.*InsufficientFreeAddressesInSubnet.*`)
	c.Check(ap.client.(*ec2stub).runInstancesCalls, check.HasLen, 4)
	metrics = arvadostest.GatherMetricsAsString(reg)
	c.Check(metrics, check.Matches, `(?ms).*`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-broken",success="0"} 2\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-broken",success="1"} 0\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-full",success="0"} 2\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-full",success="1"} 0\n`+
		`.*`)
}

func (*EC2InstanceSetSuite) TestCreateOneSubnetFailingCapacity(c *check.C) {
	if *live != "" {
		c.Skip("not applicable in live mode")
		return
	}
	ap, img, cluster, reg := GetInstanceSet(c, `{"SubnetID":["subnet-full","subnet-broken"]}`)
	ap.client.(*ec2stub).subnetErrorOnRunInstances = map[string]error{
		"subnet-full": &ec2stubError{
			Code:    "InsufficientFreeAddressesInSubnet",
			Message: "subnet is full",
		},
		"subnet-broken": &ec2stubError{
			Code:    "InsufficientInstanceCapacity",
			Message: "insufficient capacity",
		},
	}
	for i := 0; i < 3; i++ {
		_, err := ap.Create(cluster.InstanceTypes["tiny"], img, nil, "", nil)
		c.Check(err, check.NotNil)
		c.Check(err, check.ErrorMatches, `.*InsufficientInstanceCapacity.*`)
	}
	c.Check(ap.client.(*ec2stub).runInstancesCalls, check.HasLen, 6)
	metrics := arvadostest.GatherMetricsAsString(reg)
	c.Check(metrics, check.Matches, `(?ms).*`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-broken",success="0"} 3\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-broken",success="1"} 0\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-full",success="0"} 3\n`+
		`arvados_dispatchcloud_ec2_instance_starts_total{subnet_id="subnet-full",success="1"} 0\n`+
		`.*`)
}

func (*EC2InstanceSetSuite) TestTagInstances(c *check.C) {
	ap, _, _, _ := GetInstanceSet(c, "{}")
	l, err := ap.Instances(nil)
	c.Assert(err, check.IsNil)

	for _, i := range l {
		tg := i.Tags()
		tg["TestTag2"] = "123 test tag 2"
		c.Check(i.SetTags(tg), check.IsNil)
	}
}

func (*EC2InstanceSetSuite) TestListInstances(c *check.C) {
	ap, _, _, reg := GetInstanceSet(c, "{}")
	l, err := ap.Instances(nil)
	c.Assert(err, check.IsNil)

	for _, i := range l {
		tg := i.Tags()
		c.Logf("%v %v %v", i.String(), i.Address(), tg)
	}

	metrics := arvadostest.GatherMetricsAsString(reg)
	c.Check(metrics, check.Matches, `(?ms).*`+
		`arvados_dispatchcloud_ec2_instances{subnet_id="[^"]*"} \d+\n`+
		`.*`)
}

func (*EC2InstanceSetSuite) TestDestroyInstances(c *check.C) {
	ap, _, _, _ := GetInstanceSet(c, "{}")
	l, err := ap.Instances(nil)
	c.Assert(err, check.IsNil)

	for _, i := range l {
		c.Check(i.Destroy(), check.IsNil)
	}
}

func (*EC2InstanceSetSuite) TestInstancePriceHistory(c *check.C) {
	ap, img, cluster, _ := GetInstanceSet(c, "{}")
	pk, _ := test.LoadTestKey(c, "../../dispatchcloud/test/sshkey_dispatch")
	tags := cloud.InstanceTags{"arvados-ec2-driver": "test"}

	defer func() {
		instances, err := ap.Instances(tags)
		c.Assert(err, check.IsNil)
		for _, inst := range instances {
			c.Logf("cleanup: destroy instance %s", inst)
			c.Check(inst.Destroy(), check.IsNil)
		}
	}()

	ap.ec2config.SpotPriceUpdateInterval = arvados.Duration(time.Hour)
	ap.ec2config.EBSPrice = 0.1 // $/GiB/month
	inst1, err := ap.Create(cluster.InstanceTypes["tiny-preemptible"], img, tags, "true", pk)
	c.Assert(err, check.IsNil)
	defer inst1.Destroy()
	inst2, err := ap.Create(cluster.InstanceTypes["tiny-preemptible"], img, tags, "true", pk)
	c.Assert(err, check.IsNil)
	defer inst2.Destroy()

	// in live mode, we need to wait for the instances to reach
	// running state before we can discover their availability
	// zones and look up the appropriate prices.
	var instances []cloud.Instance
	for deadline := time.Now().Add(5 * time.Minute); ; {
		if deadline.Before(time.Now()) {
			c.Fatal("timed out")
		}
		instances, err = ap.Instances(tags)
		running := 0
		for _, inst := range instances {
			ec2i := inst.(*ec2Instance).instance
			if ec2i.InstanceLifecycle == types.InstanceLifecycleTypeSpot && *ec2i.State.Code&16 != 0 {
				running++
			}
		}
		if running >= 2 {
			c.Logf("instances are running, and identifiable as spot instances")
			break
		}
		c.Logf("waiting for instances to reach running state so their availability zone becomes visible...")
		time.Sleep(10 * time.Second)
	}

	for _, inst := range instances {
		hist := inst.PriceHistory(arvados.InstanceType{})
		c.Logf("%s price history: %v", inst.ID(), hist)
		c.Check(len(hist) > 0, check.Equals, true)

		histWithScratch := inst.PriceHistory(arvados.InstanceType{AddedScratch: 640 << 30})
		c.Logf("%s price history with 640 GiB scratch: %v", inst.ID(), histWithScratch)

		for i, ip := range hist {
			c.Check(ip.Price, check.Not(check.Equals), 0.0)
			if i > 0 {
				c.Check(ip.StartTime.Before(hist[i-1].StartTime), check.Equals, true)
			}
			c.Check(ip.Price < histWithScratch[i].Price, check.Equals, true)
		}
	}
}

func (*EC2InstanceSetSuite) TestWrapError(c *check.C) {
	retryError := &ec2stubError{Code: "Throttling"}
	wrapped := wrapError(retryError, &atomic.Value{})
	_, ok := wrapped.(cloud.RateLimitError)
	c.Check(ok, check.Equals, true)

	quotaError := &ec2stubError{Code: "InstanceLimitExceeded"}
	wrapped = wrapError(quotaError, nil)
	_, ok = wrapped.(cloud.QuotaError)
	c.Check(ok, check.Equals, true)

	for _, trial := range []struct {
		code               string
		msg                string
		typeSpecific       bool
		quotaGroupSpecific bool
	}{
		{
			code:               "InsufficientInstanceCapacity",
			msg:                "",
			typeSpecific:       true,
			quotaGroupSpecific: false,
		},
		{
			code:               "Unsupported",
			msg:                "Your requested instance type (t3.micro) is not supported in your requested Availability Zone (us-east-1e). Please retry your request by not specifying an Availability Zone or choosing us-east-1a, us-east-1b, us-east-1c, us-east-1d, us-east-1f.",
			typeSpecific:       true,
			quotaGroupSpecific: false,
		},
		{
			code:               "VcpuLimitExceeded",
			msg:                "You have requested more vCPU capacity than your current vCPU limit of 64 allows for the instance bucket that the specified instance type belongs to. Please visit http://aws.amazon.com/contact-us/ec2-request to request an adjustment to this limit.",
			typeSpecific:       false,
			quotaGroupSpecific: true,
		},
	} {
		capacityError := &ec2stubError{Code: trial.code, Message: trial.msg}
		wrapped = wrapError(capacityError, nil)
		caperr, ok := wrapped.(cloud.CapacityError)
		c.Check(ok, check.Equals, true)
		c.Check(caperr.IsCapacityError(), check.Equals, true)
		c.Check(caperr.IsInstanceTypeSpecific(), check.Equals, trial.typeSpecific)
		c.Check(caperr.IsInstanceQuotaGroupSpecific(), check.Equals, trial.quotaGroupSpecific)
	}
}

func (*EC2InstanceSetSuite) TestInstanceQuotaGroup(c *check.C) {
	ap, _, _, _ := GetInstanceSet(c, `{
  "InstanceTypeQuotaGroups": {
    "a": "standard",
    "m": "standard",
    "t": "standard",
    "p5": "p5"
  }
}`)

	for _, trial := range []struct {
		ptype      string
		spot       bool
		quotaGroup cloud.InstanceQuotaGroup
	}{
		{ptype: "g1.large", quotaGroup: "g"},
		{ptype: "x1.large", quotaGroup: "x"},
		{ptype: "inf1.2xlarge", quotaGroup: "inf"},
		{ptype: "a1.small", quotaGroup: "standard"},
		{ptype: "m1.xlarge", quotaGroup: "standard"},
		{ptype: "m1.xlarge", spot: true, quotaGroup: "standard-spot"},
		{ptype: "p4.xlarge", spot: true, quotaGroup: "p-spot"},
		{ptype: "p5.xlarge", spot: true, quotaGroup: "p5-spot"},
		{ptype: "t3.2xlarge", quotaGroup: "standard"},
		{ptype: "trn1.2xlarge", quotaGroup: "trn"},
		{ptype: "trn1.2xlarge", spot: true, quotaGroup: "trn-spot"},
		{ptype: "imaginary9.5xlarge", quotaGroup: "imaginary"},
		{ptype: "", quotaGroup: ""},
	} {
		c.Check(ap.InstanceQuotaGroup(arvados.InstanceType{
			ProviderType: trial.ptype,
			Preemptible:  trial.spot,
		}), check.Equals, trial.quotaGroup)
	}
}

func (*EC2InstanceSetSuite) TestAWSKeyFingerprints(c *check.C) {
	for _, keytype := range []string{"rsa", "ed25519"} {
		tmpdir := c.MkDir()
		buf, err := exec.Command("ssh-keygen", "-f", tmpdir+"/key", "-N", "", "-t", keytype).CombinedOutput()
		c.Assert(err, check.IsNil, check.Commentf("ssh-keygen: %s", buf))
		var expectfps []string
		switch keytype {
		case "rsa":
			for _, hash := range []string{"md5", "sha1"} {
				cmd := exec.Command("bash", "-c", "set -e -o pipefail; ssh-keygen -ef key -m PEM | openssl rsa -RSAPublicKey_in -outform DER | openssl "+hash+" -c")
				cmd.Dir = tmpdir
				buf, err := cmd.CombinedOutput()
				c.Assert(err, check.IsNil, check.Commentf("bash: %s", buf))
				expectfps = append(expectfps, string(regexp.MustCompile(`[0-9a-f:]{20,}`).Find(buf)))
			}
		case "ed25519":
			buf, err := exec.Command("ssh-keygen", "-l", "-f", tmpdir+"/key").CombinedOutput()
			c.Assert(err, check.IsNil, check.Commentf("ssh-keygen: %s", buf))
			sum := string(regexp.MustCompile(`SHA256:\S+`).Find(buf))
			expectfps = []string{sum + "=", sum}
		default:
			c.Error("don't know how to test fingerprint for key type " + keytype)
			continue
		}
		pk, err := libconfig.LoadSSHKey("file://" + tmpdir + "/key")
		c.Assert(err, check.IsNil)
		fingerprints, err := awsKeyFingerprints(pk.PublicKey())
		c.Assert(err, check.IsNil)
		c.Check(fingerprints, check.DeepEquals, expectfps)
	}
}
