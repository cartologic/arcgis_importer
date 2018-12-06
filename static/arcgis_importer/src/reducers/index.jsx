import { DELETE_IMPORT, IMPORTS_LOADING, IMPORT_IN_PROGRESS, RESET_IMPORT_LIST, SET_IMPORT_LIST, UPDATE_IMPORT_LIST } from '../const'

import { combineReducers } from 'redux'

let initialState = {
	imports: [],
	importsLoading: false,
	importInProgress: false,
}
export function imports(state = initialState.imports, action) {
	switch (action.type) {
		case UPDATE_IMPORT_LIST:
			return [...state, ...action.imports]
		case SET_IMPORT_LIST:
			return action.imports
		case RESET_IMPORT_LIST:
			return initialState.imports
		case DELETE_IMPORT:
			return state.filter(import_obj => !action.import_obj.id !== import_obj.id)
		default:
			return state
	}
}
export function importsLoading(state = initialState.importsLoading, action) {
	switch (action.type) {
		case IMPORTS_LOADING:
			return action.loading
		default:
			return state
	}
}
export function importInProgress(state = initialState.importInProgress, action) {
	switch (action.type) {
		case IMPORT_IN_PROGRESS:
			return action.loading
		default:
			return state
	}
}
export default combineReducers({
	imports,
	importsLoading,
	importInProgress,
})