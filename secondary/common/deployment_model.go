//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package common

import "sync"

type DeploymentModel byte

const (
	DEFAULT_DEPLOYMENT = iota
	SERVERLESS_DEPLOYMENT
)

func (b DeploymentModel) String() string {
	switch b {
	case DEFAULT_DEPLOYMENT:
		return "default"
	case SERVERLESS_DEPLOYMENT:
		return "serverless"
	default:
		return "invalid"
	}
}

func makeDeploymentModel(model string) DeploymentModel {
	if model == "serverless" {
		return SERVERLESS_DEPLOYMENT
	}
	return DEFAULT_DEPLOYMENT
}

//Global Deployment Model
var gDeploymentModel DeploymentModel
var gDeploymentModelOnce sync.Once

func GetDeploymentModel() DeploymentModel {
	return gDeploymentModel
}

func SetDeploymentModel(model string) {
	gDeploymentModelOnce.Do(func() {
		dm := makeDeploymentModel(model)
		gDeploymentModel = dm
	})
}
