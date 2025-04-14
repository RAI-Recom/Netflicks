    pipeline {
    agent any

    // environment {
    // DB_USER = credentials('DB_USER')  // ID used in Jenkins credentials
    // DB_PASSWORD = credentials('DB_PASSWORD')
    // HOST = credentials('HOST')
    // DB_PORT = credentials('DB_PORT')
    // DB_NAME = credentials('DB_NAME')
    // }

    environment {
    DB_USER     = "${env.DB_USER}"
    DB_PASSWORD = "${env.DB_PASSWORD}"
    HOST        = "${env.HOST}"
    DB_PORT     = "${env.DB_PORT}"
    DB_NAME     = "${env.DB_NAME}"
    }

    stages {
        stage('Clean Docker Images') {
            steps {
                script {
                    // Remove old Docker images to ensure fresh builds
                    sh 'docker rmi -f test1 || true'
                    sh 'docker rmi -f netflicks-infer2 || true'
                }
            }
        }

        stage('Train Model') {
            steps {
                script {
                    sh 'docker build -f Dockerfile.test1 -t test1 .'
                    
                    // Clean up any old container with the same name 'train_container'
                    sh 'docker rm -f train_container || true'

                    // sh 'docker run --network=host test1'
                    sh 'docker run --network=host --name train_container test1'

                }
            }
        }

        stage('Test Model Output') {
            steps {
                script {
                    // Copy the pickle file from the container to host (Jenkins workspace)
                    sh 'docker cp train_container:/app/models/popular_movies.pkl ./popular_movies.pkl'

                    // Check if the file exists and is non-empty
                    sh '''
                        if [ ! -s ./popular_movies.pkl ]; then
                            echo "Test failed: Pickle file is missing or empty."
                            exit 1
                        else
                            echo "Test passed: Pickle file generated successfully."
                        fi
                    '''

                    // Cleanup
                    sh 'docker rm train_container'
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
