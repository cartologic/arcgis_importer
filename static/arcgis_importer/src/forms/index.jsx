import React from 'react'
import { isURL } from '../utils'
import t from 'tcomb-form'
const URLField = t.refinement(t.String, url => {
	return isURL(url)
})
export const layerForm = t.struct({
	url: URLField,
})
export const formOptions = {
	fields: {
		url: {
			label: <i>{"Layer URL"}</i>,
			placeholder: 'Enter Your Layer URL',
			help: 'Esri Feature Layer URL Example: https://xxx/ArcGIS/rest/services/xxx/xxx/MapServer/0'
		}
	}
}