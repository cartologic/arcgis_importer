import "@babel/polyfill/noConflict"

import React, { Component } from 'react'
import { formOptions, layerForm } from '../forms'
import { importInProgress, importsLoading, setImportList } from '../actions'

import { ApiRequests } from '../utils'
import PropTypes from 'prop-types'
import { Provider } from 'react-redux'
import Spinner from 'react-spinkit'
import { connect } from 'react-redux'
import { render } from 'react-dom'
import { store } from '../store'
import t from 'tcomb-form'

function capitalizeFirstLetter(string) {
    return string.charAt(0).toUpperCase() + string.slice(1);
}

const ImportStatus = Object.freeze({
    PENDING: 'PENDING',
    IN_PROGRESS: 'IN_PROGRESS',
    FINISHED: 'FINISHED',
    FAILED: 'FAILED'
});


class ArcGISImporter extends Component {
    constructor(props) {
        super(props)
        this.state = {
            status: ImportStatus.PENDING,
            result: null
        }
        const { token, username } = this.props
        let newToken = token.split(" for ")[0]
        this.requests = new ApiRequests(username, newToken)
    }
    componentDidMount() {
        this.getImports()
    }
    getImports = () => {
        const { urls, setImports, setImportsLoading } = this.props
        setImportsLoading(true)
        this.requests.doGet(urls.importsURL).then(result => {
            setImports(result.objects)
            setImportsLoading(false)
        })
    }
    getImportStatus = (id) => {
        let that = this
        const { urls, setImportInProgress } = this.props
        this.requests.doGet(urls.importsURL + id).then(result => {
            if (result.error) {
                this.setState({ status: ImportStatus.FAILED, result: result.error }, () => setImportInProgress(false))
            } else {
                if (result.status !== ImportStatus.FINISHED && result.status != ImportStatus.FAILED) {
                    this.setState({ status: result.status, result: result.task_result }, () => {
                        setTimeout(function () { that.getImportStatus(id) }, 6000);
                    })

                } else {
                    this.setState({ status: result.status, result: result.task_result }, () => setImportInProgress(false))
                }
            }
        }).catch((error) => {
            that.setState({ status: ImportStatus.FAILED, result: error.message }, () => setImportInProgress(false))
        })
    }
    importLayer = (data) => {
        let that = this
        const { urls, setImportInProgress } = this.props
        setImportInProgress(true)
        this.requests.doPost(urls.importURL, JSON.stringify(data), {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }).then(result => {
            if (result.error) {
                this.setState({ status: ImportStatus.FAILED, result: result.error }, () => setImportInProgress(false))
            } else {
                this.getImportStatus(result.id)
            }
        }).catch((error) => {
            that.setState({ status: ImportStatus.FAILED, result: error.message }, () => setImportInProgress(false))
        })
    }
    onSubmit = (evt) => {
        evt.preventDefault()
        const value = this.form.getValue()
        if (value) {
            this.importLayer({
                url: value.url,
                permissions: permissionsString('#permission_form', 'layers')
            })
        }
    }
    render() {
        const { importsLoading, importInProgress } = this.props
        return (
            <div>
                <div className="importer-spinner">
                    {(importsLoading || importInProgress) && <Spinner className="center" name="three-bounce" />}
                </div>
                <form onSubmit={this.onSubmit}>
                    <t.form.Form ref={ref => this.form = ref} type={layerForm} options={formOptions} />
                    <div className="form-group">
                        <button disabled={importInProgress} type="submit" className="btn btn-primary">{"Import Layer"}</button>
                    </div>
                </form>
                { this.state.result && <div className={`alert alert-${this.state.status != ImportStatus.FAILED ? "success" : "danger"}`}>
                    <strong>Result:</strong> {this.state.result}
                </div>}
            </div>
        )
    }
}
ArcGISImporter.propTypes = {
    urls: PropTypes.object.isRequired,
    username: PropTypes.string.isRequired,
    token: PropTypes.string.isRequired,
    setImportsLoading: PropTypes.func.isRequired,
    setImports: PropTypes.func.isRequired,
    setImportInProgress: PropTypes.func.isRequired,
    importsLoading: PropTypes.bool.isRequired,
    imports: PropTypes.array.isRequired,
    importInProgress: PropTypes.bool.isRequired,
}
const mapStateToProps = (state) => {
    return {
        importsLoading: state.importsLoading,
        imports: state.imports,
        importInProgress: state.importInProgress,
    }
}
const mapDispatchToProps = (dispatch) => {
    return {
        setImportsLoading: (loading) => dispatch(importsLoading(loading)),
        setImports: (imports) => dispatch(setImportList(imports)),
        setImportInProgress: (loading) => dispatch(importInProgress(loading)),
    }
}
let App = connect(mapStateToProps, mapDispatchToProps)(ArcGISImporter)
global.ArcGISImporterRenderer = {
    show: (el, props) => {
        render(<Provider store={store}><App {...props} /></Provider>, document.getElementById(el))
    }
}
