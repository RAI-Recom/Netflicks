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
