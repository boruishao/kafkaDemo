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
          steps {
            sh 'echo \'test2\''
          }
        }

      }
    }

    stage('package') {
      agent {
        docker {
          image 'dockette/mvn'
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