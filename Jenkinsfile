    pipeline {
    agent any

    stages {
        stage('Run Docker Compose') {
            steps {
                script {
                    sh 'docker build -f Dockerfile.test1 -t test1 .'
                    sh 'docker run test1'
                }
            }
        }
    }
}
