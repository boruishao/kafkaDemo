pipeline {
  agent any
  stages {
    stage('test') {
      parallel {
        stage('test') {
          steps {
            echo 'i\'m testing'
          }
        }

        stage('test2') {
          agent {
            dockerfile {
              filename 'Dockerfile'
            }

          }
          steps {
            sh 'echo \'test2\''
          }
        }

      }
    }

    stage('package') {
      agent {
        dockerfile {
          filename 'Dockerfile'
        }

      }
      steps {
        sleep 2
        sh 'mvn package'
      }
    }

    stage('deploy') {
      steps {
        archiveArtifacts '*'
      }
    }

  }
}