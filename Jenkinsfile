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
                    env.API_PORT = (env.BRANCH_NAME == 'main') ? "${env.PROD_API_PORT}" : "${env.TEST_API_PORT}"
                    env.PROMETHEUS_PORT = (env.BRANCH_NAME == 'main') ? "${env.PROD_PROMETHEUS_PORT}" : "${env.TEST_PROMETHEUS_PORT}"
                    env.MLFLOW_PORT = (env.BRANCH_NAME == 'main') ? "${env.MLFLOW_PROD_PORT}" : "${env.MLFLOW_TEST_PORT}"
                    env.DOCKER_NAME_RUN = (env.BRANCH_NAME == 'main') ? 'netflicks-run' : 'netflicks_test-run'
                    env.DOCKER_NAME_TRAIN = (env.BRANCH_NAME == 'main') ? 'netflicks-train' : 'netflicks_test-train'
                    env.MODEL_VOLUME = (env.BRANCH_NAME == 'main') ? 'model_volume' : 'model_volume_test'
                    sh "echo ${env.BRANCH_NAME}"

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
                        -e MLFLOW_PORT=${MLFLOW_PORT} \
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
        
//         stage('Validate Model') {
//             steps {
//                 script {
//                     sh """
//                         docker run --rm \
//                             -v ${env.MODEL_VOLUME}:/app/models \
//                             python:3.8-slim \
//                             python3 -c '
// import pickle
// try:
//     with open("/app/models/popular_movies.pkl", "rb") as f:
//         model = pickle.load(f)
//         print("Model validation successful")
// except Exception as e:
//     print(f"Model validation failed: {str(e)}")
//     exit(1)
// '
//                     """
//                 }
//             }
//         }
        
        stage('Run Recommendation Service') {
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
                            -e API_PORT=${env.API_PORT} \
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
        stage ('Run Prometheus Service') {
            steps {
                script {
                    // Stop and remove existing container
                    sh "docker stop prometheus-development || true"
                    sh "docker rm -f prometheus-development || true"
                    
                    // Create directory to hold full Prometheus config structure
                    sh "mkdir -p /var/tmp/prometheus-configs/development"

                        writeFile file: "/var/tmp/prometheus-configs/development/prometheus.yml", text: """
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'movie_recommendation_service'
    static_configs:
      - targets:
          - 'localhost:${env.API_PORT}'
"""


                    sh "chmod 644 /var/tmp/prometheus-configs/development/prometheus.yml"

                    sh """
                        docker run -d \
                        --name prometheus-development \
                        -p ${env.PROMETHEUS_PORT}:9090 \
                        -v /var/tmp/prometheus-configs/development/prometheus.yml:/etc/prometheus/prometheus.yml \
                        --restart unless-stopped \
                        prom/prometheus \
                        --config.file=/etc/prometheus/prometheus.yml \
                        --storage.tsdb.path=/prometheus \
                        --web.console.libraries=/usr/share/prometheus/console_libraries \
                        --web.console.templates=/usr/share/prometheus/consoles
                    """

                    // Wait and check logs
                    sh "sleep 15"
                    sh "docker logs prometheus-development || true"
                }
            }
        }
        stage('Run Grafana Service') {
            steps {
                script {
                    sh "docker stop grafana || true"
                    sh "docker rm -f grafana || true"

                    sh """
                    docker run -d \
                    --name grafana \
                    -p 3000:3000 \
                    --restart unless-stopped \
                    -e GF_SECURITY_ADMIN_USER=admin \
                    -e GF_SECURITY_ADMIN_PASSWORD=admin \
                    grafana/grafana
                    """

                    sh "sleep 10"
                    sh "docker logs grafana || true"
                }
            }
        }

        // stage('Cleanup') {
        //     steps {
        //         script {
        //             if (env.BRANCH_NAME != 'main') {
        //                 sh '''
        //                     docker stop ${env.DOCKER_NAME_RUN} || true
        //                     docker rm -f ${env.DOCKER_NAME_RUN} || true
        //                     docker volume rm model_volume || true
        //                 '''
        //             }
        //         }
        //     }
        // }
    }
    
    // post {
    //     always {
    //         script {
    //                 sh '''
    //                     docker stop ${env.DOCKER_NAME_RUN} || true
    //                     docker rm -f ${env.DOCKER_NAME_RUN} || true
    //                     docker volume rm model_volume || true
    //                 '''
    //         }
    //     }
    // }
}
