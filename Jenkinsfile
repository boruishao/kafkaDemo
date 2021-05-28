pipeline {
  agent {
    docker {
      image 'centos'
    }

  }
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
      steps {
        sleep 2
      }
    }

    stage('deploy') {
      steps {
        archiveArtifacts '*'
      }
    }

  }
}