package v1alpha1

// WorkerRetirementPolicy chooses the authority that may retire deprecated builds.
// +kubebuilder:validation:Enum=Automatic;External
type WorkerRetirementPolicy string

const (
	WorkerRetirementAutomatic WorkerRetirementPolicy = "Automatic"
	WorkerRetirementExternal  WorkerRetirementPolicy = "External"
)

// WorkerVersionRetirement is an opaque, durable external release authorization.
// Retries must reuse the request ID. Removing the request before acknowledgement
// can revive the worker; external reference acquisition must stay fenced.
type WorkerVersionRetirement struct {
	// +kubebuilder:validation:MinLength=1
	BuildID string `json:"buildID"`
	// +kubebuilder:validation:MinLength=1
	RequestID string `json:"requestID"`
}

// WorkerRetirementStatus is not a Temporal execution-completion guarantee.
type WorkerRetirementStatus struct {
	Policy             WorkerRetirementPolicy `json:"policy"`
	ObservedGeneration int64                  `json:"observedGeneration"`
	// Acknowledged only after the exact Kubernetes Deployment and every
	// attached WorkerResourceTemplate copy is absent. The external owner must
	// still prove process and Temporal-version absence before releasing its fence.
	// +optional
	// +listType=map
	// +listMapKey=buildID
	RetiredVersions []WorkerVersionRetirement `json:"retiredVersions,omitempty"`
}
