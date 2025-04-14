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
                    sh 'docker rmi -f test || true'
                    sh 'docker rmi -f netflicks-infer2 || true'
                }
            }
        }

        stage('Train Model') {
            steps {
                script {
                    sh 'docker build -f Dockerfile.test -t test .'
                    
                    sh 'docker volume create model_volume'

                    // Clean up any old container
                    // sh 'docker rm -f train_container || true'
                    sh 'docker rm -f api_container || true'

                    // sh """
                    //     docker run --network=host \
                    //     --name train_container \
                    //     -e DB_USER=${env.DB_USER} \
                    //     -e DB_PASSWORD=${env.DB_PASSWORD} \
                    //     -e HOST=${env.HOST} \
                    //     -e DB_PORT=${env.DB_PORT} \
                    //     -e DB_NAME=${env.DB_NAME} \
                    //     test
                    // """
                    
                    // sh 'docker run --rm -v model_volume:/app/models test'
                    sh """
                        docker run --network=host \
                        --name api_container \
                        -p 8083:8082 \
                        -v model_volume:/app/models \
                        -e DB_USER=${env.DB_USER} \
                        -e DB_PASSWORD=${env.DB_PASSWORD} \
                        -e HOST=${env.HOST} \
                        -e DB_PORT=${env.DB_PORT} \
                        -e DB_NAME=${env.DB_NAME} \
                        test
                    """

                }
            }
        }

        stage('Test Model Output') {
            steps {
                script {
                    // Copy the pickle file from the container to host (Jenkins workspace)
                    sh 'docker cp api_container:/app/models/popular_movies.pkl ./popular_movies.pkl'

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
                    // sh 'docker rm api_container'
                }
            }
        }

        stage('Infer Model') {
            steps {
                script {
                    // sh 'docker cp trainer:/app/models ./models'
                    sh 'docker build -f Dockerfile.run -t netflicks-run .'
                    sh 'sh docker run --network=host -v model_volume:/app/models netflicks-run'
                }
            }
        }
    }
}
