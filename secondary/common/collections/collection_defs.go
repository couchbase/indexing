package collections

const COLLECTION_ID_NIL = ""
const SCOPE_ID_NIL = ""

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

func (cm *CollectionManifest) GetCollectionID(scope, collection string) string {
	for _, cmScope := range cm.Scopes {
		if cmScope.Name == scope {
			for _, cmCollection := range cmScope.Collections {
				if cmCollection.Name == collection {
					return cmCollection.UID
				}
			}
		}
	}
	return COLLECTION_ID_NIL
}

func (cm *CollectionManifest) GetScopeID(scope string) string {
	for _, cmScope := range cm.Scopes {
		if cmScope.Name == scope {
			return cmScope.UID
		}
	}
	return SCOPE_ID_NIL
}

func (cm *CollectionManifest) GetScopeAndCollectionID(scope, collection string) (string, string) {
	scopeId := SCOPE_ID_NIL
	for _, cmScope := range cm.Scopes {
		if cmScope.Name == scope {
			scopeId = cmScope.UID
			for _, cmCollection := range cmScope.Collections {
				if cmCollection.Name == collection {
					return cmScope.UID, cmCollection.UID
				}
			}
		}
	}
	return scopeId, COLLECTION_ID_NIL
}
