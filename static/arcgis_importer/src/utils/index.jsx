function updateProgress(evt) {
    if (evt.lengthComputable) {
        let percentComplete = (evt.loaded / evt.total) * 100
    }
}

function transferComplete(evt) {
    console.log("The transfer is complete.")
}
export function isURL(str) {
    var pattern = new RegExp('^(https?:\\/\\/)?' + // protocol
        '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.)+[a-z]{2,}|' + // domain name and extension
        '((\\d{1,3}\\.){3}\\d{1,3}))' + // OR ip (v4) address
        '(\\:\\d+)?' + // port
        '(\\/[-a-z\\d%@_.~+&:]*)*' + // path
        '(\\?[;&a-z\\d%@_.,~+&:=-]*)?' + // query string
        '(\\#[-a-z\\d_]*)?$', 'i'); // fragment locator
    return pattern.test(str);
}
function transferFailed(evt) {
    console.error("An error occurred while transferring the file.")
}
export function convertToSlug(Text) {
    return Text
        .toLowerCase()
        .replace(/ /g, '_')
        .replace(/[^\w-]+/g, '');
}
export class ApiRequests {
    constructor(username, token) {
        this.token = token
        this.username = username
    }
    doPost(url, data, extraHeaders = {}) {
        return fetch(url, {
            method: 'POST',
            redirect: 'follow',
            credentials: 'include',
            headers: new Headers({
                'Authorization': `ApiKey ${this.username}:${this.token}`,
                ...extraHeaders
            }),
            body: data
        }).then((response) => response.json())
    }
    doDelete(url, extraHeaders = {}) {
        return fetch(url, {
            method: 'DELETE',
            redirect: 'follow',
            credentials: 'include',
            headers: {
                'Authorization': `ApiKey ${this.username}:${this.token}`,
                ...extraHeaders
            }
        }).then((response) => response.text())
    }
    doGet(url, extraHeaders = {}) {
        return fetch(url, {
            method: 'GET',
            redirect: 'follow',
            credentials: 'include',
            headers: {
                'Authorization': `ApiKey ${this.username}:${this.token}`,
                ...extraHeaders
            }
        }).then((response) => response.json())
    }
    uploadWithProgress(url, data, resultFunc, progressFunc = updateProgress, loadFunc = transferComplete, errorFunc = transferFailed, ) {

        let xhr = new XMLHttpRequest()
        xhr.upload.addEventListener("progress", function (evt) {
            progressFunc(evt)
        }, false)
        xhr.addEventListener("load", function (evt) {
            loadFunc(xhr)
        })
        xhr.addEventListener("error", function () {
            errorFunc(xhr)
        })
        xhr.onreadystatechange = function () {
            if (xhr.readyState == XMLHttpRequest.DONE) {
                resultFunc(xhr.responseText)
            }
        }
        xhr.open('POST', url, true)
        xhr.setRequestHeader("Cache-Control", "no-cache")
        xhr.setRequestHeader('Authorization', `ApiKey ${this.username}:${this.token}`)
        xhr.send(data)

    }
}