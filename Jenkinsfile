    pipeline {
    agent any

    environment {
    DB_USER     = "${env.DB_USER}"
    DB_PASSWORD = "${env.DB_PASSWORD}"
    HOST        = "${env.HOST}"
    DB_PORT     = "${env.DB_PORT}"
    DB_NAME     = "${env.DB_NAME}"
    }

    stages {
        stage('Setup') {
            steps {
                script {
                    sh """
                        # run cleanup
                        docker rm -f netflicks-train || true
                        docker rm -f netflicks-run || true
                        docker volume rm model_volume || true
                    """

                    // Create volume and validate environment
                    sh 'docker volume create model_volume'
                    // Validate environment variables
                }
            }
        }
        
        stage('Train Model') {
            steps {
                script {
                    sh 'docker build -f Dockerfile.train -t netflicks-train .'
                    sh """
                        docker run --network=host \
                        --name netflicks-train \
                        -v model_volume:/app/models \
                        -e DB_USER=${env.DB_USER} \
                        -e DB_PASSWORD=${env.DB_PASSWORD} \
                        -e HOST=${env.HOST} \
                        -e DB_PORT=${env.DB_PORT} \
                        -e DB_NAME=${env.DB_NAME} \
                        netflicks-train
                    """
                    sh 'docker rm netflicks-train'
                }
            }
        }
        
        stage('Validate Model') {
            steps {
                script {
                    sh '''
                        # Create temporary container to validate model from volume
                        docker run --rm \
                            -v model_volume:/app/models \
                            python:3.8-slim \
                            python3 -c "
import pickle
try:
    with open('/app/models/popular_movies.pkl', 'rb') as f:
        model = pickle.load(f)
        print('Model validation successful')
except Exception as e:
    print(f'Model validation failed: {str(e)}')
    exit(1)
"
                    '''
                }
            }
        }
        
        stage('Run Service') {
            steps {
                script {
                    sh '''
                        # Build the service image
                        docker build -f Dockerfile.run -t netflicks-run .
                        
                        # Run the Flask API service
                        docker run -d \
                            --name netflicks-run \
                            -p 8082:8082 \
                            -v model_volume:/app/models \
                            -e DB_USER=${env.DB_USER} \
                            -e DB_PASSWORD=${env.DB_PASSWORD} \
                            -e HOST=172.17.0.1 \
                            -e DB_PORT=${env.DB_PORT} \
                            -e DB_NAME=${env.DB_NAME} \
                            netflicks-run
                        
                        # Wait for the service to be ready
                        timeout=60
                        while [ $timeout -gt 0 ]; do
                            if curl -s http://localhost:8082/health > /dev/null; then
                                echo "Service is up and running"
                                break
                            fi
                            sleep 5
                            timeout=$((timeout-5))
                        done
                        
                        if [ $timeout -eq 0 ]; then
                            echo "Service failed to start within timeout"
                            docker logs netflicks-run
                            exit 1
                        fi
                    '''
                }
            }
        }
        
        stage('Cleanup') {
            steps {
                script {
                    sh '''
                        # Always run cleanup
                        docker rm -f netflicks-train || true
                        docker rm -f netflicks-run || true
                        docker volume rm model_volume || true
                    '''
                }
            }
        }
    }
}
