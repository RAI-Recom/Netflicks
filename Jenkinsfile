pipeline {
    agent any

    stages {
        // stage('Checkout') {
        //     steps {
        //         scm
        //     }
        // }
        // stage('Build Docker Image') {
        //     steps {
        //         script {
        //             sh 'docker build -t myapp:latest .'
        //         }
        //     }
        // }
        stage('Run Docker Compose') {
            steps {
                script {
                    sh 'docker build -f Dockerfile.test1 -t test1 .'
                }
            }
        }
    }
}
