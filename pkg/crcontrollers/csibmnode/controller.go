/*
Copyright © 2020 Dell Inc. or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package csibmnode

import (
	"context"
	"reflect"
	"sync"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	api "github.com/dell/csi-baremetal/api/generated/v1"
	nodecrd "github.com/dell/csi-baremetal/api/v1/csibmnodecrd"
	"github.com/dell/csi-baremetal/pkg/base/k8s"
)

const (
	// NodeIDAnnotationKey hold key for annotation for node object
	NodeIDAnnotationKey = "dell.csi-baremetal.node/id"
)

// Controller is a controller for CSIBMNode CR
type Controller struct {
	k8sClient *k8s.KubeClient
	cache     *nodesCache
	handler   *k8sNodeEventHandler

	log *logrus.Entry
}

// nodesCache holds mapping between names for k8s node and BMCSINode CR objects
type nodesCache struct {
	k8sToBMNode map[string]string // k8s CSIBMNode name to CSIBMNode CR name
	bmToK8sNode map[string]string // CSIBMNode CR name to k8s CSIBMNode name
	sync.RWMutex
}

func (nc *nodesCache) getK8sNodeName(bmNodeName string) (string, bool) {
	nc.RLock()
	res, ok := nc.bmToK8sNode[bmNodeName]
	nc.Unlock()

	return res, ok
}

func (nc *nodesCache) getCSIBMNodeName(k8sNodeName string) (string, bool) {
	nc.RLock()
	res, ok := nc.k8sToBMNode[k8sNodeName]
	nc.Unlock()

	return res, ok
}

func (nc *nodesCache) put(k8sNodeName, bmNodeName string) {
	nc.Lock()
	nc.k8sToBMNode[k8sNodeName] = bmNodeName
	nc.bmToK8sNode[bmNodeName] = k8sNodeName
	nc.Unlock()
}

// NewController returns instance of Controller
func NewController(k8sClient *k8s.KubeClient, logger *logrus.Logger) (*Controller, error) {
	cache := &nodesCache{
		k8sToBMNode: make(map[string]string),
		bmToK8sNode: make(map[string]string),
	}
	return &Controller{
		k8sClient: k8sClient,
		cache:     cache,
		handler: &k8sNodeEventHandler{
			k8sClient: k8sClient,
			cache:     cache,
			logger:    logger.WithField("component", "k8sNodeEventHandler"),
		},
		log: logger.WithField("component", "Controller"),
	}, nil
}

// SetupWithManager registers Controller to k8s controller manager
func (bmc *Controller) SetupWithManager(m ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(m).
		For(&nodecrd.CSIBMNode{}). // primary resource
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1, // reconcile all object by turn, concurrent reconciliation isn't supported
		}).
		Watches(&source.Kind{Type: &coreV1.Node{}}, &handler.Funcs{
			CreateFunc: bmc.handler.createFunc,
			UpdateFunc: bmc.handler.updateFunc,
		}). // secondary resource
		WithEventFilter(predicate.Funcs{
			UpdateFunc: func(e event.UpdateEvent) bool {
				if _, ok := e.ObjectOld.(*nodecrd.CSIBMNode); ok {
					return true
				}

				nodeOld := e.ObjectOld.(*coreV1.Node)
				nodeNew := e.ObjectNew.(*coreV1.Node)

				shouldReconcile := !reflect.DeepEqual(nodeOld.GetAnnotations(), nodeNew.GetAnnotations()) ||
					!reflect.DeepEqual(nodeOld.Status.Addresses, nodeNew.Status.Addresses)
				bmc.log.Debugf("UpdateEvent for k8s node %s. Trigger reconcile - %v.", nodeOld.Name, shouldReconcile)

				return shouldReconcile
			},
		}).
		Complete(bmc)
}

// Reconcile reconcile CSIBMNode CR and k8s CSIBMNode objects
// at first define for which object current Reconcile is triggered and then run corresponding reconciliation method
func (bmc *Controller) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ll := bmc.log.WithFields(logrus.Fields{
		"method": "Reconcile",
		"name":   req.Name,
	})

	var err error
	// try to read CSIBMNode
	bmNode := new(nodecrd.CSIBMNode)
	if err = bmc.k8sClient.ReadCR(context.Background(), req.Name, bmNode); err != nil {
		ll.Errorf("Unable to read corresponding CSIBMNode CR: %v", err)
		return ctrl.Result{Requeue: true}, err
	}

	// get corresponding k8s node name from cache
	var k8sNode = &coreV1.Node{}
	if k8sNodeName, ok := bmc.cache.getK8sNodeName(bmNode.Name); ok {
		ll.Debugf("Found k8s node name in cache - %s", k8sNodeName)
		if err := bmc.k8sClient.ReadCR(context.Background(), k8sNodeName, k8sNode); err != nil {
			ll.Errorf("Unable to read k8s node %s: %v", k8sNodeName, err)
			return ctrl.Result{Requeue: true}, err
		}
		return bmc.checkAnnotation(k8sNode, bmNode.Spec.UUID)
	}

	// search corresponding k8s node name in k8s API
	ll.Debug("Read k8s nodes from API")
	k8sNodes := new(coreV1.NodeList)
	if err := bmc.k8sClient.ReadList(context.Background(), k8sNodes); err != nil {
		ll.Errorf("Unable to read k8s nodes list: %v", err)
		return ctrl.Result{Requeue: true}, err
	}

	matchedNodes := make([]string, 0)
	for i := range k8sNodes.Items {
		matchedAddresses := matchedAddressesCount(bmNode, &k8sNodes.Items[i])
		if matchedAddresses == len(bmNode.Spec.Addresses) {
			k8sNode = &k8sNodes.Items[i]
			matchedNodes = append(matchedNodes, k8sNode.Name)
			continue
		}
		if matchedAddresses > 0 {
			ll.Warnf("There is k8s node %s that partially match CSIBMNode CR %s. CSIBMNode.Spec: %v, k8s node addresses: %v",
				k8sNodes.Items[i].Name, bmNode.Name, bmNode.Spec, k8sNodes.Items[i].Status.Addresses)
			return ctrl.Result{Requeue: false}, nil
		}
	}

	if len(matchedNodes) != 1 {
		ll.Warnf("Unable to detect k8s node that corresponds to CSIBMNode %v, matched nodes: %v", bmNode, matchedNodes)
		return ctrl.Result{}, nil
	}

	bmc.cache.put(k8sNode.Name, bmNode.Name)
	return bmc.checkAnnotation(k8sNode, bmNode.Spec.UUID)
}

// checkAnnotation checks NodeIDAnnotationKey annotation value for provided k8s CSIBMNode and compare that value with goalValue
// update k8s CSIBMNode object if needed, method is used as a last step of Reconcile
func (bmc *Controller) checkAnnotation(k8sNode *coreV1.Node, goalValue string) (ctrl.Result, error) {
	ll := bmc.log.WithField("method", "checkAnnotation")
	val, ok := k8sNode.GetAnnotations()[NodeIDAnnotationKey]
	switch {
	case ok && val == goalValue:
		// nothing to do
	case ok && val != goalValue:
		ll.Warnf("%s value for node %s is %s, however should have (according to corresponding CSIBMNode's UUID) %s, going to update annotation's value.",
			NodeIDAnnotationKey, k8sNode.Name, val, goalValue)
		fallthrough
	default:
		if k8sNode.ObjectMeta.Annotations == nil {
			k8sNode.ObjectMeta.Annotations = make(map[string]string, 1)
		}
		k8sNode.ObjectMeta.Annotations[NodeIDAnnotationKey] = goalValue
		if err := bmc.k8sClient.UpdateCR(context.Background(), k8sNode); err != nil {
			ll.Errorf("Unable to update node object: %v", err)
			return ctrl.Result{Requeue: true}, err
		}
	}

	return ctrl.Result{}, nil
}

// matchedAddressesCount return amount of k8s node addresses that has corresponding address in bmNodeCR.Spec.Addresses map
func matchedAddressesCount(bmNodeCR *nodecrd.CSIBMNode, k8sNode *coreV1.Node) int {
	matchedCount := 0
	for _, addr := range k8sNode.Status.Addresses {
		crAddr, ok := bmNodeCR.Spec.Addresses[string(addr.Type)]
		if ok && crAddr == addr.Address {
			matchedCount++
		}
	}

	return matchedCount
}

// constructAddresses converts k8sNode.Status.Addresses into the the map[string]string, key - address type, value - address
func constructAddresses(k8sNode *coreV1.Node) map[string]string {
	res := make(map[string]string, len(k8sNode.Status.Addresses))
	for _, addr := range k8sNode.Status.Addresses {
		res[string(addr.Type)] = addr.Address
	}

	return res
}

type k8sNodeEventHandler struct {
	k8sClient *k8s.KubeClient
	cache     *nodesCache
	logger    *logrus.Entry
}

func (h *k8sNodeEventHandler) createFunc(e event.CreateEvent, q workqueue.RateLimitingInterface) {
	ll := h.logger.WithFields(logrus.Fields{
		"method": "createFunc",
		"name":   e.Meta.GetName(),
	})

	ll.Info("Processing")

	node, ok := e.Object.(*coreV1.Node)
	if !ok {
		ll.Errorf("Got event for object that isn't a k8s node: %v. Meta: %v", e.Object, e.Meta)
		return
	}

	ll.Infof("Search in cache by key %s", e.Meta.GetName())
	bmNodeName, ok := h.cache.getCSIBMNodeName(e.Meta.GetName())
	ll.Infof("Got value - %s, ok - %v", bmNodeName, ok)
	if ok {
		ll.Infof("Put in queue event for CSIBMNode %s", bmNodeName)
		q.Add(bmNodeName)
		return
	}

	bmNodes := &nodecrd.CSIBMNodeList{}
	if err := h.k8sClient.List(context.Background(), bmNodes); err != nil {
		ll.Errorf("Unable to read bmNodes list: %v", err)
		q.Add(e.Meta.GetName())
		return
	}
	ll.Infof("Read %d CSIBMNodes", len(bmNodes.Items))

	for _, bmNode := range bmNodes.Items {
		ll.Debugf("compare with CSIBMNode %s", bmNode.Name)
		if len(node.Status.Addresses) == matchedAddressesCount(&bmNode, node) {
			enqueueForKey := "default/" + bmNode.Name
			ll.Infof("CSIBMNode %s is corresponds to k8s node %s. Add it to queue and in cache.", enqueueForKey, e.Meta.GetName())
			h.cache.put(e.Meta.GetName(), bmNode.Name)
			q.Add(enqueueForKey)
		}
	}

	bmNodeName = uuid.New().String()
	bmNode := h.k8sClient.ConstructCSIBMNodeCR(bmNodeName, api.CSIBMNode{
		UUID:      uuid.New().String(),
		Addresses: constructAddresses(node),
	})
	ll.Infof("Going to create CSIBMNode %s: %v", bmNodeName, bmNode)

	if err := h.k8sClient.CreateCR(context.Background(), bmNodeName, bmNode); err != nil {
		ll.Errorf("Unable to create CSIBMNode CR %s: %v. Requeue for k8s node", bmNodeName, err)
		q.Add(e.Meta.GetName())
		return
	}
	h.cache.put(e.Meta.GetName(), bmNode.Name)
}

func (h *k8sNodeEventHandler) updateFunc(e event.UpdateEvent, q workqueue.RateLimitingInterface) {
	ll := h.logger.WithFields(logrus.Fields{
		"method": "updateFunc",
		"name":   e.MetaOld.GetName(),
	})

	ll.Info("Processing")

	k8sNode, ok := e.ObjectNew.(*coreV1.Node)
	if !ok {
		ll.Errorf("Got event for object that isn't a k8s node: %v. Meta: %v", e.ObjectNew)
		return
	}

	ll.Infof("Search in cache by key %s", e.MetaOld.GetName())
	bmNodeName, ok := h.cache.getCSIBMNodeName(e.MetaOld.GetName())
	ll.Infof("Got value - %s, ok - %v", bmNodeName, ok)
	if ok {
		ll.Infof("Put in queue event for CSIBMNode %s", bmNodeName)
		q.Add(bmNodeName)
		return
	}

	bmNodes := &nodecrd.CSIBMNodeList{}
	if err := h.k8sClient.List(context.Background(), bmNodes); err != nil {
		ll.Errorf("Unable to read bmNodes list: %v", err)
		q.Add(e.MetaOld.GetName())
		return
	}
	ll.Infof("Read %d CSIBMNodes", len(bmNodes.Items))

	for _, bmNode := range bmNodes.Items {
		if len(k8sNode.Status.Addresses) == matchedAddressesCount(&bmNode, k8sNode) {
			enqueueForKey := "default/" + bmNode.Name
			ll.Infof("CSIBMNode %s is corresponds to k8s node %s. Add to it to queue.", enqueueForKey, e.MetaOld.GetName())
			q.Add(enqueueForKey)
		}
	}
}
