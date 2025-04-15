# Netflicks - Movie Recommendation System

A machine learning-based movie recommendation system that provides personalized movie suggestions to users. The system consists of multiple components including data preprocessing, model training, and a REST API for serving recommendations.

## System Architecture

The Netflicks recommendation system follows a microservices architecture with the following key components:

### 1. Data Pipeline
- **Data Collection**: Raw movie and user data is collected and stored in the `data/` directory
- **Kafka Integration**: Real-time data streaming through Kafka for continuous data updates
- **Preprocessing**: Data cleaning, transformation, and feature engineering in the `preprocessing/` module
- **Database**: Persistent storage of processed data in the `db/` directory

### 2. Model Training Pipeline
- **Feature Engineering**: Creation of user and movie embeddings
- **Model Training**: Training of recommendation algorithms in `model_training/`
- **Model Storage**: Trained models are saved in the `models/` directory
- **Model Evaluation**: Performance metrics and validation in `pipeline_testing/`

### 3. API Service
- **Flask Server**: REST API implementation in `api/`
- **Model Serving**: Real-time inference using trained models
- **Docker Containerization**: Isolated deployment environment
- **Load Balancing**: Handles multiple concurrent requests

### 4. Testing & Quality Assurance
- **Unit Tests**: Component-level testing in `testing/`
- **Pipeline Testing**: End-to-end testing of the recommendation pipeline
- **CI/CD**: Automated testing and deployment through Jenkins

### Data Flow
1. Raw data → Kafka → Preprocessing → Database
2. Database → Model Training → Model Storage
3. API Request → Model Inference → Recommendation Response

### Deployment Architecture
```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Data Source│────▶│  Kafka      │────▶│Preprocessing│
└─────────────┘     └─────────────┘     └─────────────┘
                           │                   │
                           ▼                   ▼
                    ┌─────────────┐     ┌─────────────┐
                    │  Database   │◀────│  Model      │
                    │             │     │  Training   │
                    └─────────────┘     └─────────────┘
                           │                   │
                           ▼                   ▼
                    ┌─────────────┐     ┌─────────────┐
                    │   API       │◀────│  Model      │
                    │   Service   │     │  Storage    │
                    └─────────────┘     └─────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │   Client    │
                    │  Requests   │
                    └─────────────┘
```

## Project Structure

```
.
├── api/                 # API server implementation
├── data/               # Data storage directory
├── db/                 # Database related files
├── kafka_import/       # Kafka integration for data streaming
├── model_training/     # Model training scripts
├── models/            # Trained model storage
├── pipeline_testing/  # Testing utilities
├── preprocessing/     # Data preprocessing scripts
└── testing/          # Test files
```

## Prerequisites

- Python 3.10
- Docker
- pip

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd Netflicks
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.run.txt  # For running the API
pip install -r requirements.train.txt  # For training the model
pip install -r requirements.data.txt  # For data processing
```

## Environment Setup

Create a `.env` file in the root directory with the following variables:
```
# Add your environment variables here
```

## Running the Application

### Using Docker

1. Build the Docker image:
```bash
docker build -f Dockerfile.run -t netflicks-api .
```

2. Run the container:
```bash
docker run -p 8082:8082 -v $(pwd)/models:/app/models netflicks-api
```

### Running Locally

1. Start the API server:
```bash
python api/server.py
```

The API will be available at `http://localhost:8082`

## API Endpoints

- `GET /recommend/{user_id}`: Get movie recommendations for a specific user

## Development

### Training Pipeline

1. Data Preprocessing:
```bash
python preprocessing/preprocess.py
```

2. Model Training:
```bash
python model_training/train.py
```

### Testing

Run tests using:
```bash
python -m pytest testing/
```

## CI/CD

The project uses Jenkins for continuous integration and deployment. The pipeline configuration is defined in the `Jenkinsfile`.

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here] 