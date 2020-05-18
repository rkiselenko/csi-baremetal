package common

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	k8smetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "eos2git.cec.lab.emc.com/ECS/baremetal-csi-plugin.git/api/generated/v1"
	apiV1 "eos2git.cec.lab.emc.com/ECS/baremetal-csi-plugin.git/api/v1"
	crdV1 "eos2git.cec.lab.emc.com/ECS/baremetal-csi-plugin.git/api/v1"
	accrd "eos2git.cec.lab.emc.com/ECS/baremetal-csi-plugin.git/api/v1/availablecapacitycrd"
	"eos2git.cec.lab.emc.com/ECS/baremetal-csi-plugin.git/api/v1/lvgcrd"
	vcrd "eos2git.cec.lab.emc.com/ECS/baremetal-csi-plugin.git/api/v1/volumecrd"
	"eos2git.cec.lab.emc.com/ECS/baremetal-csi-plugin.git/pkg/base/util"
)

var (
	testLogger = logrus.New()
	testNS     = "default"

	testCtx       = context.Background()
	testNode1Name = "node1"
	testNode2Name = "node2"

	testDrive1UUID = "drive1-uuid"
	testDrive2UUID = "drive2-uuid"
	testDrive3UUID = "drive3-uuid"
	testDrive4UUID = "drive4-uuid"

	// Available Capacity variables
	testAC1Name = fmt.Sprintf("%s-%s", testNode1Name, strings.ToLower(testDrive1UUID))
	testAC1     = accrd.AvailableCapacity{
		TypeMeta:   k8smetav1.TypeMeta{Kind: "AvailableCapacity", APIVersion: crdV1.APIV1Version},
		ObjectMeta: k8smetav1.ObjectMeta{Name: testAC1Name, Namespace: testNS},
		Spec: api.AvailableCapacity{
			Size:         int64(util.GBYTE),
			StorageClass: apiV1.StorageClassHDD,
			Location:     testDrive1UUID,
			NodeId:       testNode1Name},
	}
	testAC2Name = fmt.Sprintf("%s-%s", testNode2Name, strings.ToLower(testDrive2UUID))
	testAC2     = accrd.AvailableCapacity{
		TypeMeta:   k8smetav1.TypeMeta{Kind: "AvailableCapacity", APIVersion: crdV1.APIV1Version},
		ObjectMeta: k8smetav1.ObjectMeta{Name: testAC2Name, Namespace: testNS},
		Spec: api.AvailableCapacity{
			Size:         int64(util.GBYTE) * 100,
			StorageClass: apiV1.StorageClassHDD,
			Location:     testDrive2UUID,
			NodeId:       testNode2Name,
		},
	}
	testAC3Name = fmt.Sprintf("%s-%s", testNode2Name, strings.ToLower(testDrive3UUID))
	testAC3     = accrd.AvailableCapacity{
		TypeMeta:   k8smetav1.TypeMeta{Kind: "AvailableCapacity", APIVersion: crdV1.APIV1Version},
		ObjectMeta: k8smetav1.ObjectMeta{Name: testAC3Name, Namespace: testNS},
		Spec: api.AvailableCapacity{
			Size:         int64(util.TBYTE),
			StorageClass: apiV1.StorageClassHDD,
			Location:     testDrive3UUID,
			NodeId:       testNode2Name,
		},
	}
	testAC4Name = fmt.Sprintf("%s-%s", testNode2Name, strings.ToLower(testDrive4UUID))
	testAC4     = accrd.AvailableCapacity{
		TypeMeta:   k8smetav1.TypeMeta{Kind: "AvailableCapacity", APIVersion: crdV1.APIV1Version},
		ObjectMeta: k8smetav1.ObjectMeta{Name: testAC4Name, Namespace: testNS},
		Spec: api.AvailableCapacity{
			Size:         testLVG.Spec.Size,
			StorageClass: apiV1.StorageClassHDDLVG,
			Location:     testLVGName,
			NodeId:       testNode2Name,
		},
	}

	// LVG variables
	testLVGName = "lvg-1"
	testLVG     = lvgcrd.LVG{
		TypeMeta:   k8smetav1.TypeMeta{Kind: "LVG", APIVersion: crdV1.APIV1Version},
		ObjectMeta: k8smetav1.ObjectMeta{Name: testLVGName, Namespace: testNS},
		Spec: api.LogicalVolumeGroup{
			Name:      testLVGName,
			Node:      testNode2Name,
			Locations: []string{testDrive4UUID},
			Size:      int64(util.GBYTE) * 100,
			Status:    crdV1.Creating,
		},
	}

	// Volumes variables
	testVolume1Name = "aaaa-1111"
	testVolume1     = vcrd.Volume{
		TypeMeta: k8smetav1.TypeMeta{Kind: "Volume", APIVersion: crdV1.APIV1Version},
		ObjectMeta: k8smetav1.ObjectMeta{
			Name:              testVolume1Name,
			Namespace:         testNS,
			CreationTimestamp: k8smetav1.Time{Time: time.Now()},
		},
		Spec: api.Volume{
			Id:           testVolume1Name,
			NodeId:       testNode1Name,
			Size:         int64(util.GBYTE),
			StorageClass: apiV1.StorageClassHDD,
			Location:     testDrive1UUID,
			CSIStatus:    crdV1.Creating,
		},
	}
)
