# SDG4 Education Inequity Analysis

**[View Live Interactive Dashboard](https://sdg4-inequity.streamlit.app/)** 

## Problem Statement

Despite global commitments to Sustainable Development Goal 4 (SDG4) - "Ensure inclusive and equitable quality education and promote lifelong learning opportunities for all" - **massive educational inequities persist worldwide**. These inequities manifest across multiple dimensions:

### **The Challenge**
- **Geographic Disparities**: Rural vs. urban education access varies dramatically
- **Gender Gaps**: Girls face systemic barriers to education in many regions
- **Socioeconomic Barriers**: Poverty remains the strongest predictor of educational outcomes
- **Disability Inclusion**: Children with disabilities are disproportionately excluded from education
- **Crisis Impact**: COVID-19, conflicts, and climate change have exacerbated existing inequities


## 📊 What This Project Delivers

### **Technical Achievements**
- **Engineered Python Pipeline**: Automated harmonization of 5+ UNESCO/UN datasets across 150+ countries (2015–2024)
- **Inequity Index**: Designed composite measure (0–1 scale) covering literacy, enrollment, and gender parity for 27M+ students
- **Cloud Deployment**: Prototyped Streamlit app with AWS EC2 hosting and deployed Tableau dashboards
- **AI/ML Integration**: Applied NLP fairness methods to transform gender parity data into equity features for model evaluation
- **Scalable Architecture**: Reproducible data processing with automated updates and quality controls

### **Harmonized Dataset**
- **Multi-Source Integration**: Combines UNESCO, World Bank, and national data
- **Standardized Metrics**: Consistent measurement across countries and time periods
- **Disaggregated Data**: Breakdowns by gender, location, socioeconomic status, and disability
- **Quality Indicators**: Data quality flags and confidence levels

### **Key Indicators Tracked**
- **SDG 4.1.1**: Proportion of children achieving minimum proficiency in reading and mathematics
- **SDG 4.2.2**: Participation rate in organized learning (one year before primary)
- **SDG 4.5.1**: Gender parity indices for education access and completion
- **SDG 4.a.1**: Proportion of schools with access to electricity and internet
- **SDG 4.c.1**: Proportion of teachers with minimum qualifications

### **Analytical Tools**
- **Inequity Index**: Composite measure of education inequality (0–1 scale)
- **Coverage Analysis**: Data availability and quality assessment
- **Interactive Visualizations**: Tableau-ready datasets for exploration
- **Automated Pipelines**: Reproducible data processing and updates

## 🚀 Getting Started

### **Prerequisites**
- Python 3.12+
- Virtual environment (recommended)

### **Installation**
```bash
# Clone the repository
git clone https://github.com/leahdsouza/sdg4-inequity.git
cd sdg4-inequity

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up pre-commit hooks
pre-commit install
```

### **Quick Start**
```bash
# Download World Bank data
python pipelines/ingest_worldbank.py SE.PRM.CMPT.ZS

# Process and harmonize data
python pipelines/harmonize.py

# Build inequity index
python pipelines/build_index.py

# Export for visualization
python pipelines/export_for_tableau.py
```


## 📈 Project Outcomes

### **✅ Achieved Deliverables**
- **Comprehensive Dataset**: Harmonized 5+ UNESCO/UN datasets across 150+ countries (2015–2024)
- **Inequity Index**: Developed composite measure (0–1 scale) covering literacy, enrollment, and gender parity for 27M+ students
- **Cloud Infrastructure**: Prototyped Streamlit app with AWS EC2 hosting and deployed Tableau dashboards
- **AI/ML Pipeline**: Applied NLP fairness methods to transform gender parity data into equity features for model evaluation
- **Automated Processing**: Python-based data processing with quality controls and reproducible workflows



## Acknowledgments

- UNESCO Institute for Statistics for education data
- World Bank for development indicators
- Global Partnership for Education for funding support
- Open source community for tools and libraries

---

*Source: UNESCO Institute for Statistics, 2023*
