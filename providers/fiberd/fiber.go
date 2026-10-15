// Copyright (c) 2026. MIT License - see LICENSE file for details.

package fiberd

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	workspacev1alpha1 "github.com/orka-agents/orka-workspace/api/v1alpha1"
	"github.com/orka-agents/orka-workspace/providers/fiberd/payload"
	workspaceprovider "github.com/orka-agents/orka-workspace/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Fibers is the fiberd home that runs supervisor fibers. Every instance gets
// its own grant, so its run directory, Watch status and capacity are its own.
type Fibers interface {
	// Clone admits the instance's grant and creates or attaches its session.
	// A retry after a lost response attaches the same running fiber. The
	// home starts no fiber for this call after deadline.
	Clone(ctx context.Context, grant FiberGrant, launch []byte, deadline time.Time) (Fiber, error)
	// Release ends the fiber; an unknown fiber is already gone.
	Release(ctx context.Context, fiberID string) error
	// Running reports how many fibers the grant runs.
	Running(ctx context.Context, grantUID string) (int, error)
	// Challenge reads the supervisor's public sealed bootstrap challenge
	// through the fiber's relay address.
	Challenge(ctx context.Context, relay string) (BootstrapChallenge, error)
	// Home is the Pod that hosts every fiber, Core's infrastructure worker,
	// with the labels that select it.
	Home(ctx context.Context) (workspaceprovider.PodReference, map[string]string, error)
	// Images are the supervisor image references the home's rootfs serves.
	Images() []string
	// Capabilities are what every sandbox's init holds, as CAP_ names.
	Capabilities() []string
}

// FiberGrant names one instance's grant and session on the home.
type FiberGrant struct {
	UID     string
	Tenant  string
	Session string
}

// Fiber is a running fiber as Clone reports it. Relay is the home's
// host:port that fiberd relays to the fiber's unix socket.
type Fiber struct {
	ID    string
	Relay string
	Epoch int64
}

// ErrChallengeClosed reports a bootstrap listener Core already consumed.
var ErrChallengeClosed = errors.New("supervisor bootstrap listener closed")

// BootstrapChallenge mirrors the supervisor's public sealed bootstrap
// challenge. Signing, sealing and credentials stay in Core.
type BootstrapChallenge struct {
	Schema    string `json:"schema"`
	Nonce     string `json:"nonce"`
	BootNonce string `json:"bootNonce"`
	PublicKey string `json:"publicKey"`
	Actor     struct {
		Atespace string `json:"atespace"`
		Name     string `json:"name"`
		UID      string `json:"uid"`
	} `json:"actor"`
}

// fiberRecord is the journaled fiber of one instance. The grant UID is the
// instance ID, so a new sequence never shares a run directory or fence.
// fiberd relays each fiber on its own home port, while Core dials the
// admitted bootstrap port; a per-instance Service maps one to the other.
type fiberRecord struct {
	Grant           string                          `json:"grant"`
	Home            workspaceprovider.PodReference  `json:"home"`
	Process         payload.Identity                `json:"process"`
	FiberID         string                          `json:"fiberID,omitempty"`
	Relay           string                          `json:"relay,omitempty"`
	Epoch           int64                           `json:"epoch,omitempty"`
	Service         *workspaceprovider.PodReference `json:"service,omitempty"`
	ChallengeSHA256 string                          `json:"challengeSHA256,omitempty"`
	// CloneDeadline is journaled before every Clone is sent. Until it
	// passes, a fiber absent from the home may still be starting.
	CloneDeadline time.Time `json:"cloneDeadline,omitzero"`
}

const (
	nativeUIDEnv          = "ORKA_ACP_POD_UID"
	bootstrapNonceEnv     = "ORKA_ACP_CREDENTIAL_BOOTSTRAP_NONCE"
	sealedBootstrapSchema = "orka.harness.v2/sealed-bootstrap/v1"
	fiberTenantMaxLength  = 253
	fiberServiceLabel     = "fiberd.workspace.orka.ai/instance"
	// cloneBudget bounds each Clone; cloneGrace covers clock skew and
	// transit before retirement trusts that an issued Clone cannot land.
	cloneBudget = 30 * time.Second
	cloneGrace  = 10 * time.Second
)

func validateFiberRequest(request workspaceprovider.WorkloadRequest, images, capabilities []string) error {
	if request.RestoreFrom != nil {
		return fmt.Errorf("fiberd provider does not support checkpoint import")
	}
	if request.Runtime == nil {
		return nil
	}
	runtime := request.Runtime
	for _, feature := range runtime.RequiredFeatures {
		switch feature {
		case workspacev1alpha1.WorkspaceFeatureACPRuntime, workspacev1alpha1.WorkspaceFeaturePools, workspacev1alpha1.WorkspaceFeatureNativeProcess:
		default:
			return fmt.Errorf("fiberd runtime does not support required feature %q", feature)
		}
	}
	if !slices.Contains(runtime.RequiredFeatures, workspacev1alpha1.WorkspaceFeatureNativeProcess) {
		return fmt.Errorf("fiberd runs supervisors as native processes; the class must require %q", workspacev1alpha1.WorkspaceFeatureNativeProcess)
	}
	if err := validateFiberStorage(request); err != nil {
		return err
	}
	if runtime.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		return fmt.Errorf("fiberd runtime requires restartPolicy Never; another process needs a new workload sequence")
	}
	if runtime.BootstrapPort != 80 {
		return fmt.Errorf("fiberd supervisors serve the native bootstrap port 80")
	}
	if len(runtime.Template.Spec.Containers) != 1 || runtime.Template.Spec.Containers[0].Name != runtime.ContainerName || len(runtime.Template.Spec.InitContainers) != 0 {
		return fmt.Errorf("fiberd runs exactly one supervisor container")
	}
	// A fiber is restored from the home's template image; Clone cannot select
	// another image. Only the image the home's rootfs was built from is admitted.
	if !slices.Contains(images, request.Image) {
		return fmt.Errorf("fiberd home does not serve supervisor image %q", request.Image)
	}
	// The template starts the supervisor in the sandbox root; the payload
	// carries no working directory.
	if dir := runtime.Template.Spec.Containers[0].WorkingDir; dir != "" && dir != "/" {
		return fmt.Errorf("fiberd supervisors start in /, not working directory %q", dir)
	}
	if err := validateFiberSecurity(runtime.Template.Spec, capabilities); err != nil {
		return err
	}
	if _, err := launchFor(request, payload.Identity{Namespace: "x", Name: "x", UID: "x"}); err != nil {
		return err
	}
	return nil
}

// validateFiberSecurity admits only what a sandbox restored from the home's
// template holds: uid 0 inside the sandbox and the home's capability set.
func validateFiberSecurity(spec corev1.PodSpec, capabilities []string) error {
	var runAsUser *int64
	var runAsNonRoot *bool
	if spec.SecurityContext != nil {
		runAsUser, runAsNonRoot = spec.SecurityContext.RunAsUser, spec.SecurityContext.RunAsNonRoot
	}
	sc := spec.Containers[0].SecurityContext
	if sc != nil {
		if sc.RunAsUser != nil {
			runAsUser = sc.RunAsUser
		}
		if sc.RunAsNonRoot != nil {
			runAsNonRoot = sc.RunAsNonRoot
		}
	}
	if (runAsUser != nil && *runAsUser != 0) || (runAsNonRoot != nil && *runAsNonRoot) {
		return fmt.Errorf("fiberd supervisors run as uid 0 inside their sandbox")
	}
	if sc == nil {
		return nil
	}
	if sc.Privileged != nil && *sc.Privileged {
		return fmt.Errorf("fiberd supervisors run unprivileged inside their sandbox")
	}
	if sc.Capabilities == nil {
		return nil
	}
	// Every sandbox holds exactly the home's capabilities. A request may
	// add only those, and may not drop one it does not add back; a dropped
	// capability would still be held. A named drop wins over an add.
	held := map[string]bool{}
	dropAll := slices.ContainsFunc(sc.Capabilities.Drop, func(c corev1.Capability) bool { return capabilityName(c) == "CAP_ALL" })
	for _, name := range capabilities {
		held[name] = !dropAll
	}
	for _, add := range sc.Capabilities.Add {
		if !slices.Contains(capabilities, capabilityName(add)) {
			return fmt.Errorf("fiberd home sandboxes do not hold capability %s", add)
		}
		held[capabilityName(add)] = true
	}
	for _, drop := range sc.Capabilities.Drop {
		if name := capabilityName(drop); name != "CAP_ALL" {
			held[name] = false
		}
	}
	for _, name := range capabilities {
		if !held[name] {
			return fmt.Errorf("fiberd home sandboxes hold capability %s, which the request drops", name)
		}
	}
	return nil
}

func capabilityName(c corev1.Capability) string {
	name := strings.ToUpper(string(c))
	if !strings.HasPrefix(name, "CAP_") {
		name = "CAP_" + name
	}
	return name
}

// validateFiberStorage rejects every volume. The launch payload carries no
// mounts, so the fixed template could never materialize one; Core already
// drops scratch emptyDirs from native process templates.
func validateFiberStorage(request workspaceprovider.WorkloadRequest) error {
	if request.Runtime == nil {
		return nil
	}
	spec := request.Runtime.Template.Spec
	if len(spec.Volumes) != 0 {
		return fmt.Errorf("fiberd runtime volume %q is unsupported; fibers have no mounts", spec.Volumes[0].Name)
	}
	for _, container := range spec.Containers {
		if len(container.VolumeMounts) != 0 || len(container.VolumeDevices) != 0 {
			return fmt.Errorf("fiberd supervisor container %q mounts storage a fiber cannot provide", container.Name)
		}
	}
	return nil
}

// launchFor translates the admitted container into the template's payload.
// Environment must be literal: a fiber has no downward API.
func launchFor(request workspaceprovider.WorkloadRequest, identity payload.Identity) ([]byte, error) {
	container := request.Runtime.Template.Spec.Containers[0]
	if len(container.EnvFrom) != 0 {
		return nil, fmt.Errorf("fiberd supervisors take literal environment only")
	}
	launch := payload.Launch{Version: payload.Version, Identity: identity, Command: request.Command, Args: request.Args}
	for _, variable := range container.Env {
		if variable.ValueFrom != nil {
			return nil, fmt.Errorf("fiberd supervisors take literal environment only; %s uses valueFrom", variable.Name)
		}
		launch.Env = append(launch.Env, variable.Name+"="+variable.Value)
	}
	return payload.Encode(launch)
}

func envValue(request workspaceprovider.WorkloadRequest, name string) string {
	for _, variable := range request.Runtime.Template.Spec.Containers[0].Env {
		if variable.Name == name {
			return variable.Value
		}
	}
	return ""
}

func newFiberRecord(request workspaceprovider.WorkloadRequest, identity workspaceprovider.InstanceIdentity) *fiberRecord {
	sum := sha256.Sum256([]byte(identity.InstanceID + "\x00" + string(request.Key.WorkspaceUID)))
	return &fiberRecord{
		Grant: identity.InstanceID,
		Process: payload.Identity{
			Namespace: request.Key.Namespace,
			Name:      identity.InstanceID,
			UID:       "fiber-" + hex.EncodeToString(sum[:16]),
		},
	}
}

func (d *Lifecycle) ensureFiber(ctx context.Context, record *journalRecord, save func() error) (workspaceprovider.AllocationObservation, error) {
	f := record.Fiber
	if f == nil {
		return workspaceprovider.AllocationObservation{}, fmt.Errorf("runtime journal has no fiber intent")
	}
	if f.FiberID == "" {
		home, _, err := d.fibers.Home(ctx)
		if err != nil {
			return pendingObservation(record), err
		}
		if record.CreateIssued && f.Home != home {
			return pendingObservation(record), fmt.Errorf("fiber home changed before the clone outcome was recorded: %w", workspaceprovider.ErrStaleIdentity)
		}
		launch, err := launchFor(record.Request, f.Process)
		if err != nil {
			return workspaceprovider.AllocationObservation{}, err
		}
		if !record.CreateIssued {
			f.Home = home
			record.CreateIssued = true
		}
		// The deadline is durable before the Clone is sent, and a journal
		// conflict with retirement stops this call before it clones.
		f.CloneDeadline = d.now().Add(cloneBudget)
		if err := save(); err != nil {
			return workspaceprovider.AllocationObservation{}, err
		}
		// A retry of the named session attaches the fiber a lost response
		// created; no evidence of an unrecorded fiber was ever published.
		fiber, err := d.fibers.Clone(ctx, FiberGrant{UID: f.Grant, Tenant: tenantOf(record.Request.Key.Namespace), Session: f.Grant}, launch, f.CloneDeadline)
		if err != nil {
			return pendingObservation(record), err
		}
		f.FiberID, f.Relay, f.Epoch = fiber.ID, fiber.Relay, fiber.Epoch
		if err := save(); err != nil {
			return workspaceprovider.AllocationObservation{}, err
		}
	}
	if f.Service == nil {
		service, err := d.ensureService(ctx, record)
		if err != nil {
			return pendingObservation(record), err
		}
		f.Service = service
		if err := save(); err != nil {
			if apierrors.IsConflict(err) {
				d.dropLateService(ctx, record.Request.Key, f.Grant, service)
			}
			return workspaceprovider.AllocationObservation{}, err
		}
	}
	observed, err := d.observeFiber(ctx, record)
	if err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	if f.ChallengeSHA256 == "" && observed.Startup != nil {
		f.ChallengeSHA256 = observed.Startup.Process.ChallengeSHA256
	}
	record.Observation = observed
	if err := save(); err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	return observed, nil
}

func (d *Lifecycle) observeFiber(ctx context.Context, record *journalRecord) (workspaceprovider.AllocationObservation, error) {
	f := record.Fiber
	if f == nil || f.FiberID == "" || f.Service == nil {
		return pendingObservation(record), nil
	}
	home, labels, err := d.fibers.Home(ctx)
	if err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	if home != f.Home {
		return workspaceprovider.AllocationObservation{}, fmt.Errorf("fiber home was replaced; its fibers ended with it: %w", workspaceprovider.ErrStaleIdentity)
	}
	// Core dials the Service, not the relay; evidence stands only while the
	// recorded Service still routes the bootstrap port to this fiber.
	service := &corev1.Service{}
	if err := d.client.Get(ctx, types.NamespacedName{Namespace: f.Service.Namespace, Name: f.Service.Name}, service); err != nil {
		if apierrors.IsNotFound(err) {
			return workspaceprovider.AllocationObservation{}, fmt.Errorf("fiber Service %s is gone: %w", f.Service.Name, workspaceprovider.ErrStaleIdentity)
		}
		return workspaceprovider.AllocationObservation{}, err
	}
	if service.UID != f.Service.UID || service.DeletionTimestamp != nil {
		return workspaceprovider.AllocationObservation{}, fmt.Errorf("fiber Service %s was replaced: %w", f.Service.Name, workspaceprovider.ErrStaleIdentity)
	}
	if err := checkService(service, record, labels); err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	running, err := d.fibers.Running(ctx, f.Grant)
	if err != nil {
		return workspaceprovider.AllocationObservation{}, err
	}
	if running != 1 {
		if f.ChallengeSHA256 != "" {
			return workspaceprovider.AllocationObservation{}, fmt.Errorf("recorded fiber stopped; uncertain work cannot be replayed")
		}
		return pendingObservation(record), nil
	}
	hash, err := d.challenge(ctx, record)
	if err != nil {
		if errors.Is(err, workspaceprovider.ErrStaleIdentity) {
			return workspaceprovider.AllocationObservation{}, err
		}
		// The supervisor may still be starting behind its bridged endpoint.
		return pendingObservation(record), nil
	}
	observed := record.Observation
	observed.State = workspaceprovider.AllocationReady
	observed.Startup = &workspaceprovider.StartupEvidence{
		ContractVersion: workspaceprovider.LifecycleContractV1,
		Identity:        observed.Identity,
		Endpoint:        serviceEndpoint(f.Service, record.Request.Runtime.BootstrapPort),
		Process: &workspacev1alpha1.NativeProcessEvidence{
			Namespace: f.Process.Namespace, Name: f.Process.Name, UID: f.Process.UID,
			Version: max(f.Epoch, 1), Worker: f.Home, ChallengeSHA256: hash,
		},
	}
	return observed, nil
}

// challenge pins the supervisor's one-time challenge to this record. After
// Core consumes it the listener closes and the recorded digest stands.
func (d *Lifecycle) challenge(ctx context.Context, record *journalRecord) (string, error) {
	f := record.Fiber
	challenge, err := d.fibers.Challenge(ctx, f.Relay)
	if errors.Is(err, ErrChallengeClosed) && f.ChallengeSHA256 != "" {
		return f.ChallengeSHA256, nil
	}
	if err != nil {
		return "", err
	}
	key, keyErr := base64.RawURLEncoding.DecodeString(challenge.PublicKey)
	boot, bootErr := base64.RawURLEncoding.DecodeString(challenge.BootNonce)
	nonce := envValue(record.Request, bootstrapNonceEnv)
	if nonce == "" || challenge.Schema != sealedBootstrapSchema || challenge.Nonce != nonce ||
		challenge.Actor.Atespace != f.Process.Namespace || challenge.Actor.Name != f.Process.Name || challenge.Actor.UID != f.Process.UID ||
		keyErr != nil || len(key) != 32 || bootErr != nil || len(boot) != 32 {
		return "", workspaceprovider.ErrStaleIdentity
	}
	data, err := json.Marshal(challenge)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	hash := "sha256:" + hex.EncodeToString(sum[:])
	if f.ChallengeSHA256 != "" && f.ChallengeSHA256 != hash {
		return "", fmt.Errorf("supervisor process changed: %w", workspaceprovider.ErrStaleIdentity)
	}
	return hash, nil
}

// stopFiber releases the exact fiber and reports true once the grant runs
// none. An unrecorded clone is resolved through the grant it alone uses.
// Retirement first waits out the last journaled Clone deadline, since a
// Clone already sent may still start a fiber. Then a running fiber is
// attached through its named session, recorded and released; the grant
// admits one fiber, so attaching never creates another.
func (d *Lifecycle) stopFiber(ctx context.Context, record *journalRecord, save func() error) (bool, error) {
	f := record.Fiber
	if f == nil {
		return false, fmt.Errorf("runtime journal has no fiber intent")
	}
	if !record.CreateIssued {
		return true, nil
	}
	if f.FiberID == "" {
		if d.now().Before(f.CloneDeadline.Add(cloneGrace)) {
			return false, nil
		}
		running, err := d.fibers.Running(ctx, f.Grant)
		if err != nil {
			return false, err
		}
		if running != 0 {
			launch, err := launchFor(record.Request, f.Process)
			if err != nil {
				return false, err
			}
			f.CloneDeadline = d.now().Add(cloneBudget)
			if err := save(); err != nil {
				return false, err
			}
			fiber, err := d.fibers.Clone(ctx, FiberGrant{UID: f.Grant, Tenant: tenantOf(record.Request.Key.Namespace), Session: f.Grant}, launch, f.CloneDeadline)
			if err != nil {
				return false, err
			}
			f.FiberID, f.Relay, f.Epoch = fiber.ID, fiber.Relay, fiber.Epoch
			if err := save(); err != nil {
				return false, err
			}
		}
	}
	if f.FiberID != "" {
		if err := d.fibers.Release(ctx, f.FiberID); err != nil {
			return false, err
		}
	}
	running, err := d.fibers.Running(ctx, f.Grant)
	if err != nil || running != 0 {
		return false, err
	}
	return d.deleteService(ctx, record)
}

func pendingObservation(record *journalRecord) workspaceprovider.AllocationObservation {
	observed := record.Observation
	observed.State = workspaceprovider.AllocationPending
	observed.Startup = nil
	return observed
}

// tenantOf maps a namespace to a fiberd tenant segment.
func tenantOf(namespace string) string {
	if len(namespace) > fiberTenantMaxLength {
		return namespace[:fiberTenantMaxLength]
	}
	return strings.TrimSpace(namespace)
}

func serviceName(record *journalRecord) string { return record.Fiber.Grant }

func serviceEndpoint(service *workspaceprovider.PodReference, port int32) string {
	return fmt.Sprintf("http://%s.%s.svc:%d", service.Name, service.Namespace, port)
}

// ensureService maps the admitted bootstrap port to the fiber's relay port on
// exactly the recorded home Pod.
func (d *Lifecycle) ensureService(ctx context.Context, record *journalRecord) (*workspaceprovider.PodReference, error) {
	f := record.Fiber
	home, labels, err := d.fibers.Home(ctx)
	if err != nil {
		return nil, err
	}
	if home != f.Home || len(labels) == 0 {
		return nil, fmt.Errorf("fiber home changed before its Service existed: %w", workspaceprovider.ErrStaleIdentity)
	}
	_, relayPort, err := net.SplitHostPort(f.Relay)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(relayPort)
	if err != nil {
		return nil, err
	}
	desired := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: home.Namespace, Name: serviceName(record), Labels: map[string]string{fiberServiceLabel: f.Grant}},
		Spec: corev1.ServiceSpec{Selector: labels, Ports: []corev1.ServicePort{{
			Name: "supervisor", Protocol: corev1.ProtocolTCP, Port: record.Request.Runtime.BootstrapPort, TargetPort: intstr.FromInt(port),
		}}},
	}
	if d.beforeServiceCreate != nil {
		d.beforeServiceCreate()
	}
	if err := d.client.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, err
	}
	actual := &corev1.Service{}
	if err := d.client.Get(ctx, client.ObjectKeyFromObject(desired), actual); err != nil {
		return nil, err
	}
	if err := checkService(actual, record, labels); err != nil {
		return nil, err
	}
	return &workspaceprovider.PodReference{Namespace: actual.Namespace, Name: actual.Name, UID: actual.UID}, nil
}

// dropLateService deletes a Service whose journal update lost a race.
// Retirement may have run since the fiber was recorded and found no Service;
// one it never saw must not outlive the instance. A concurrent ensure of the
// same instance may have saved this very Service, so it is kept while the
// journal still ensures that instance, and when the journal cannot be read.
func (d *Lifecycle) dropLateService(ctx context.Context, key workspaceprovider.AllocationKey, grant string, service *workspaceprovider.PodReference) {
	_, current, err := d.read(ctx, key)
	if err != nil && !errors.Is(err, workspaceprovider.ErrNotFound) {
		return
	}
	if err == nil && current.Operation == "ensure" && current.Fiber != nil && current.Fiber.Grant == grant &&
		current.Observation.State != workspaceprovider.AllocationStopped && current.Observation.State != workspaceprovider.AllocationDeleted {
		return
	}
	uid := service.UID
	_ = d.client.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: service.Namespace, Name: service.Name}},
		&client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
}

// checkService reports whether service routes the instance's bootstrap port
// to its fiber's relay port on the home Pod that labels select.
func checkService(service *corev1.Service, record *journalRecord, labels map[string]string) error {
	f := record.Fiber
	_, relayPort, err := net.SplitHostPort(f.Relay)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(relayPort)
	if err != nil {
		return err
	}
	if service.Labels[fiberServiceLabel] != f.Grant || len(labels) == 0 || !reflect.DeepEqual(service.Spec.Selector, labels) || len(service.Spec.Ports) != 1 ||
		service.Spec.Ports[0].Protocol != corev1.ProtocolTCP || service.Spec.Ports[0].Port != record.Request.Runtime.BootstrapPort ||
		service.Spec.Ports[0].TargetPort != intstr.FromInt(port) {
		return fmt.Errorf("fiber Service %s differs from its instance: %w", service.Name, workspaceprovider.ErrStaleIdentity)
	}
	return nil
}

// deleteService removes the recorded Service by UID and reports its absence.
func (d *Lifecycle) deleteService(ctx context.Context, record *journalRecord) (bool, error) {
	f := record.Fiber
	home := f.Home
	if f.Service != nil {
		home.Namespace = f.Service.Namespace
	}
	if home.Namespace == "" {
		return true, nil
	}
	service := &corev1.Service{}
	err := d.client.Get(ctx, types.NamespacedName{Namespace: home.Namespace, Name: serviceName(record)}, service)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if service.Labels[fiberServiceLabel] != f.Grant || (f.Service != nil && service.UID != f.Service.UID) {
		return false, workspaceprovider.ErrStaleIdentity
	}
	uid := service.UID
	if err := d.client.Delete(ctx, service, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}
	return false, nil
}
