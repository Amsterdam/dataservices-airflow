#!groovy

// trigger this pipeline also by time (besides triggering it by a merge)
properties(
    [
        pipelineTriggers([cron('0 9,12 * * 5,3')])
    ]
)

// get pipeline run cause description
def isUser = currentBuild.getBuildCauses()[0].shortDescription
def isTimer = currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause')


def tryStep(String message, Closure block, Closure tearDown = null) {
    try {
        block()
    }
    catch (Throwable t) {
        slackSend message: "${env.JOB_NAME}: ${message} failure ${env.BUILD_URL}", channel: '#ci-channel', color: 'danger'

        throw t
    }
    finally {
        if (tearDown) {
            tearDown()
        }
    }
}

node {
    stage("Checkout") {
        checkout scm
        echo "isUser = ${isUser}"
        echo "isTimer = ${isTimer}"
    }

//For now, there is nothing to test
    /* stage('Test') { */
    /*     tryStep "test", { */
    /*         sh "docker-compose -p dataservices_airflow -f src/.jenkins/test/docker-compose.yml build --no-cache && " + */
    /*            "docker-compose -p dataservicesgob_airflow -f src/.jenkins/test/docker-compose.yml run -u root --rm test" */

    /*     }, { */
    /*         sh "docker-compose -p dataservicesgob_airflow -f src/.jenkins/test/docker-compose.yml down" */
    /*     } */
    /* } */

    stage("Build image") {
        tryStep "build", {
            docker.withRegistry("${DOCKER_REGISTRY_HOST}", 'docker_registry_auth') {
                def image = docker.build("datapunt/dataservices/dataservices_airflow:${env.BUILD_NUMBER}",
                    "--no-cache " +
                    "--shm-size 1G " +
                    "--build-arg BUILD_ENV=acc" +
                    " src")
                image.push()
            }
        }
    }
}


String BRANCH  = "${env.BRANCH_NAME}"
String IS_USER  = "${isUser}"
String IS_TIMER = "${isTimer}"


if (BRANCH == "master") {

    node {
        stage('Push acceptance image') {
            tryStep "image tagging", {
                docker.withRegistry("${DOCKER_REGISTRY_HOST}", 'docker_registry_auth') {
                    def image = docker.image("datapunt/dataservices/dataservices_airflow:${env.BUILD_NUMBER}")
                    image.pull()
                    image.push("acceptance")
                }
            }
        }
    }

    node {
        stage("Deploy to ACC") {
            tryStep "deployment", {
                build job: 'Subtask_Openstack_Playbook',
                    parameters: [
                        [$class: 'StringParameterValue', name: 'INVENTORY', value: 'acceptance'],
                        [$class: 'StringParameterValue', name: 'PLAYBOOK', value: 'deploy.yml'],
                        [$class: 'StringParameterValue', name: 'PLAYBOOKPARAMS', value: "-e cmdb_id=app_airflow_v2"]
                    ]
            }
        }
    }

    // Only ask for manual approval when committing on this repo.
    if (IS_USER == "Push event to branch master") {
        stage('Waiting for approval') {
            slackSend channel: '#ci-channel', color: 'warning', message: 'dataservices_airflow service is waiting for Production Release - please confirm'
            input "Deploy to Production?"
        }
    }

    node {
        stage('Push production image') {
            tryStep "image tagging", {
                docker.withRegistry("${DOCKER_REGISTRY_HOST}", 'docker_registry_auth') {
                    def image = docker.image("datapunt/dataservices/dataservices_airflow:${env.BUILD_NUMBER}")
                    image.pull()
                    image.push("production")
                    image.push("latest")
                }
            }
        }
    }

    node {
        stage("Deploy to PRD") {
            tryStep "deployment", {
                build job: 'Subtask_Openstack_Playbook',
                    parameters: [
                        [$class: 'StringParameterValue', name: 'INVENTORY', value: 'production'],
                        [$class: 'StringParameterValue', name: 'PLAYBOOK', value: 'deploy.yml'],
                        [$class: 'StringParameterValue', name: 'PLAYBOOKPARAMS', value: "-e cmdb_id=app_airflow_v2"]
                    ]
            }
        }
    }
}

