package collections

type CollectionManifest struct {
	UID    string            `json:"uid"`
	Scopes []CollectionScope `json:"scopes"`
}

type CollectionScope struct {
	Name        string       `json:"name"`
	UID         string       `json:"uid"` // base 16 string
	Collections []Collection `json:"collections"`
}

type Collection struct {
	Name string `json:"name"`
	UID  string `json:"uid"` // base-16 string
}
