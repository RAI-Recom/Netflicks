    pipeline {
    agent any

    stages {
        stage('Train Model') {
            steps {
                script {
                    sh 'docker build -f Dockerfile.test1 -t test1 .'
                    sh 'docker run --network=host test1'
                }
            }
        }
        stage('Infer Model') {
            steps {
                script {
                    sh 'docker build -f Dockerfile.infer2 -t netflicks-infer2 .'
                    sh 'docker run --network=host netflicks-infer2'
                }
            }
        }
    }
}
