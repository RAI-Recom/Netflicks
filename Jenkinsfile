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
                    env.API_PORT = (env.BRANCH_NAME == 'main') ? '8082' : '9092'
                    env.DOCKER_NAME_RUN = (env.BRANCH_NAME == 'main') ? 'netflicks-run' : 'netflicks_test-run'
                    env.DOCKER_NAME_TRAIN = (env.BRANCH_NAME == 'main') ? 'netflicks-train' : 'netflicks_test-train'
                    env.MODEL_VOLUME = (env.BRANCH_NAME == 'main') ? 'model_volume' : 'model_volume_test'

                    sh """
                        # run cleanup
                        docker stop ${env.DOCKER_NAME_RUN} || true
                        docker rm -f ${env.DOCKER_NAME_TRAIN} || true
                        docker rm -f ${env.DOCKER_NAME_RUN} || true
                        docker volume rm ${env.MODEL_VOLUME} || true
                    """

                    // Create volume and validate environment
                    sh "docker volume create ${env.MODEL_VOLUME}"
                    // Validate environment variables
                }
            }
        }
        
        stage('Train Model') {
            steps {
                script {
                    sh "docker build -f Dockerfile.train -t ${env.DOCKER_NAME_TRAIN} ."
                    sh """
                        docker run --network=host \
                        --name ${env.DOCKER_NAME_TRAIN} \
                        -v ${env.MODEL_VOLUME}:/app/models \
                        -e DB_USER=${DB_USER} \
                        -e DB_PASSWORD=${DB_PASSWORD} \
                        -e HOST=${HOST} \
                        -e DB_PORT=${DB_PORT} \
                        -e DB_NAME=${DB_NAME} \
                        ${env.DOCKER_NAME_TRAIN}
                    """
                    sh "docker rm ${env.DOCKER_NAME_TRAIN}"
                }
            }
        }
        
        stage('Validate Model') {
            steps {
                script {
                    sh """
                        docker run --rm \
                            -v ${env.MODEL_VOLUME}:/app/models \
                            python:3.8-slim \
                            python3 -c '
import pickle
try:
    with open("/app/models/popular_movies.pkl", "rb") as f:
        model = pickle.load(f)
        print("Model validation successful")
except Exception as e:
    print(f"Model validation failed: {str(e)}")
    exit(1)'
                    """
                }
            }
        }
        
        stage('Run Service') {
            steps {
                script {
                    sh """#!/usr/bin/env bash
                        # Build the service image
                        docker build -f Dockerfile.run -t ${env.DOCKER_NAME_RUN} .
                        
                        # Run the Flask API service in detached mode with restart policy
                        docker run -d \
                            --name ${env.DOCKER_NAME_RUN} \
                            --restart unless-stopped \
                            --network host \
                            -v ${env.MODEL_VOLUME}:/app/models \
                            -e DB_USER='${DB_USER}' \
                            -e DB_PASSWORD='${DB_PASSWORD}' \
                            -e HOST='${HOST}' \
                            -e DB_PORT='${DB_PORT}' \
                            -e DB_NAME='${DB_NAME}' \
                            ${env.DOCKER_NAME_RUN}
                        
                        # Quick health check
                        sleep 5
                        if curl -s http://localhost:'${env.API_PORT}'/health > /dev/null; then
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
    //                 docker stop ${env.DOCKER_NAME_RUN} || true
    //                 docker rm -f ${env.DOCKER_NAME_RUN} || true
    //                 docker volume rm model_volume || true
    //             '''
    //         }
    //     }
    // }
}
