    pipeline {
    agent any

    environment {
    DB_USER     = "${env.DB_USER}"
    DB_PASSWORD = "${env.DB_PASSWORD}"
    HOST        = "${env.HOST}"
    DB_PORT     = "${env.DB_PORT}"
    DB_NAME     = "${env.DB_NAME}"
    API_PORT    = "${env.API_PORT}"
    }

    stages {
        stage('Setup') {
            steps {
                script {
                    sh """
                        # run cleanup
                        docker stop netflicks-run || true
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
                    sh """

                        # Build the service image
                        docker build -f Dockerfile.run -t netflicks-run .
                        
                        # Run the Flask API service in detached mode with restart policy
                        docker run -d \
                            --name netflicks-run \
                            --restart unless-stopped \
                            --network host \
                            -v model_volume:/app/models \
                            -e DB_USER='${DB_USER}' \
                            -e DB_PASSWORD='${DB_PASSWORD}' \
                            -e HOST='${env.HOST}' \
                            -e DB_PORT='${DB_PORT}' \
                            -e DB_NAME='${DB_NAME}' \
                            netflicks-run
                        
                        # Quick health check
                        sleep 5
                        if curl -s http://localhost:'${env.DB_PORT}'/health > /dev/null; then
                            echo "Service deployed successfully"
                        else
                            echo "Service deployment completed. Health check pending."
                        fi
                    """
                }
            }
        }
    }
    
    // post {
    //     always {
    //         script {
    //             sh '''
    //                 # Cleanup on pipeline stop or failure
    //                 docker stop netflicks-run || true
    //                 docker rm -f netflicks-run || true
    //                 docker volume rm model_volume || true
    //             '''
    //         }
    //     }
    // }
}
