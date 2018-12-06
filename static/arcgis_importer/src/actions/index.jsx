import {
    DELETE_IMPORT,
    IMPORTS_LOADING,
    IMPORT_IN_PROGRESS,
    RESET_IMPORT_LIST,
    SET_IMPORT_LIST,
    UPDATE_IMPORT_LIST
} from '../const'
export function importInProgress(loading) {
    return {
        type: IMPORT_IN_PROGRESS,
        loading
    }
}
export function importsLoading(loading) {
    return {
        type: IMPORTS_LOADING,
        loading
    }
}
export function setImportList(imports) {
    return {
        type: SET_IMPORT_LIST,
        imports
    }
}
export function updateImports(imports) {
    return {
        type: UPDATE_IMPORT_LIST,
        imports
    }
}
export function resetImports(imports) {
    return {
        type: RESET_IMPORT_LIST,
        imports
    }
}
export function deleteImport(import_obj) {
    return {
        type: DELETE_IMPORT,
        import_obj
    }
}