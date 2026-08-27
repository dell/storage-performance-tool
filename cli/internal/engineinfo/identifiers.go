package engineinfo

import "strconv"

const buildReferencePrefix = "build-"

func canonicalBuildReference(index int) string {
	return buildReferencePrefix + strconv.Itoa(index+1)
}
