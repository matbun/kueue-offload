package shadow

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"

	"github.com/containerd/containerd/log"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

// endpointSliceManager marks the EndpointSlices a shadow publishes itself with,
// which the cluster's EndpointSlice controller leaves alone.
const endpointSliceManager = "kueue-offload-executor"

// endpointSlice publishes pod, the shadow's, to svc as the EndpointSlice
// controller would publish a pod that svc selects, with the ports of svc that
// the shadow forwards. Nil when svc reaches none of them.
func endpointSlice(svc corev1.Service, name, workloadUID string, ports []portMapping, pod *corev1.Pod) *discoveryv1.EndpointSlice {
	var slicePorts []discoveryv1.EndpointPort
	for _, p := range svc.Spec.Ports {
		protocol := p.Protocol
		if protocol == "" {
			protocol = corev1.ProtocolTCP
		}
		target := p.Port
		switch {
		case p.TargetPort.Type == intstr.String:
			i := slices.IndexFunc(ports, func(m portMapping) bool { return m.Name == p.TargetPort.StrVal && m.Protocol == string(protocol) })
			if i < 0 {
				continue
			}
			target = ports[i].Port
		case p.TargetPort.IntVal != 0:
			target = p.TargetPort.IntVal
		}
		if !slices.ContainsFunc(ports, func(m portMapping) bool { return m.Port == target && m.Protocol == string(protocol) }) {
			continue
		}
		slicePorts = append(slicePorts, discoveryv1.EndpointPort{Name: ptr.To(p.Name), Port: ptr.To(target), Protocol: ptr.To(protocol), AppProtocol: p.AppProtocol})
	}
	if len(svc.Spec.Ports) > 0 && len(slicePorts) == 0 {
		return nil
	}
	addressType := discoveryv1.AddressTypeIPv4
	if ip := net.ParseIP(pod.Status.PodIP); ip != nil && ip.To4() == nil {
		addressType = discoveryv1.AddressTypeIPv6
	}
	ready := podReady(pod)
	endpoint := discoveryv1.Endpoint{
		Addresses: []string{pod.Status.PodIP},
		Conditions: discoveryv1.EndpointConditions{
			Ready:       ptr.To(ready || svc.Spec.PublishNotReadyAddresses),
			Serving:     ptr.To(ready),
			Terminating: ptr.To(false),
		},
		TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
	}
	if pod.Spec.NodeName != "" {
		endpoint.NodeName = ptr.To(pod.Spec.NodeName)
	}
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name + "-" + svc.Name,
			Namespace: svc.Namespace,
			Labels: map[string]string{
				discoveryv1.LabelServiceName: svc.Name,
				discoveryv1.LabelManagedBy:   endpointSliceManager,
			},
		},
		AddressType: addressType,
		Endpoints:   []discoveryv1.Endpoint{endpoint},
		Ports:       slicePorts,
	}
	setLabels(&slice.ObjectMeta, ownerLabels(workloadUID))
	return slice
}

// applyEndpointSlices makes the workload's EndpointSlices the desired ones, one
// per Service, and deletes the others.
func (m *manager) applyEndpointSlices(ctx context.Context, workloadUID string, desired []*discoveryv1.EndpointSlice) error {
	slicesAPI := m.client.DiscoveryV1().EndpointSlices(m.namespace)
	list, err := slicesAPI.List(ctx, metav1.ListOptions{LabelSelector: labels.Set(ownerLabels(workloadUID)).String()})
	if err != nil {
		return fmt.Errorf("failed to list the endpoint slices of workload %s: %w", workloadUID, err)
	}
	current := map[string]*discoveryv1.EndpointSlice{}
	for i := range list.Items {
		current[list.Items[i].Labels[discoveryv1.LabelServiceName]] = &list.Items[i]
	}
	var errs []error
	for _, want := range desired {
		service := want.Labels[discoveryv1.LabelServiceName]
		hash, err := stampHash(&want.ObjectMeta, []any{want.AddressType, want.Endpoints, want.Ports})
		if err != nil {
			return err
		}
		have := current[service]
		delete(current, service)
		switch {
		case have != nil && have.Annotations[specHashAnnotation] == hash:
			continue
		case have != nil && have.AddressType == want.AddressType && have.Name == want.Name:
			want.ResourceVersion = have.ResourceVersion
			if _, err := slicesAPI.Update(ctx, want, metav1.UpdateOptions{}); err != nil {
				errs = append(errs, fmt.Errorf("failed to update endpoint slice %s/%s: %w", m.namespace, want.Name, err))
			}
			continue
		case have != nil:
			if err := m.deleteEndpointSlice(ctx, have); err != nil {
				errs = append(errs, err)
				continue
			}
		}
		if _, err := slicesAPI.Create(ctx, want, metav1.CreateOptions{}); err != nil {
			if apierrors.IsAlreadyExists(err) {
				err = fmt.Errorf("%w: endpoint slice %s/%s exists", ErrNameTaken, m.namespace, want.Name)
			}
			errs = append(errs, fmt.Errorf("failed to publish the shadow to service %s/%s: %w", m.namespace, service, err))
			continue
		}
		log.G(ctx).Infof("Published shadow pod %s to service %s/%s", want.Endpoints[0].Addresses[0], m.namespace, service)
	}
	for _, stale := range current {
		if err := m.deleteEndpointSlice(ctx, stale); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *manager) deleteEndpointSlice(ctx context.Context, slice *discoveryv1.EndpointSlice) error {
	uid := slice.UID
	err := m.client.DiscoveryV1().EndpointSlices(m.namespace).Delete(ctx, slice.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete endpoint slice %s/%s: %w", m.namespace, slice.Name, err)
	}
	return nil
}
