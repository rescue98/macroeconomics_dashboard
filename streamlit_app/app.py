import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import os
from utils.data_queries import DataQueries
from utils.model_loader import ModelLoader
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="Economic Data Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 0.5rem;
    }
    
    .sidebar-content {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)

# Initialize data queries and model loader
@st.cache_resource
def init_connections():
    """Initialize database and model connections"""
    try:
        data_queries = DataQueries()
        model_loader = ModelLoader()
        return data_queries, model_loader
    except Exception as e:
        st.error(f"Error initializing connections: {str(e)}")
        return None, None

def main():
    """Main application function"""
    
    # Header
    st.markdown('<div class="main-header">Economic Data Analytics Dashboard</div>', 
                unsafe_allow_html=True)
    
    # Initialize connections
    data_queries, model_loader = init_connections()
    
    if data_queries is None:
        st.error("Failed to connect to database. Please check your configuration.")
        return
    
    # Sidebar
    with st.sidebar:
        st.markdown('<div class="sidebar-content">', unsafe_allow_html=True)
        st.header("📊 Navigation")
        
        page = st.selectbox(
            "Choose a page:",
            ["Overview", "World Bank Analysis", "Local Data Analysis", 
             "Economic Insights", "ML Predictions", "Data Quality"]
        )
        
        st.header("⚙️ Filters")
        
        # Year selection
        available_years = data_queries.get_available_years()
        if available_years:
            selected_years = st.multiselect(
                "Select Years:",
                available_years,
                default=available_years[-3:] if len(available_years) >= 3 else available_years
            )
        else:
            selected_years = []
            st.warning("No data available")
        
        # Country selection for World Bank data
        if page in ["World Bank Analysis", "Economic Insights", "ML Predictions"]:
            available_countries = data_queries.get_available_countries()
            if available_countries:
                selected_countries = st.multiselect(
                    "Select Countries:",
                    available_countries,
                    default=available_countries[:10] if len(available_countries) >= 10 else available_countries
                )
            else:
                selected_countries = []
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Main content based on page selection
    if page == "Overview":
        show_overview(data_queries, selected_years)
    elif page == "World Bank Analysis":
        show_worldbank_analysis(data_queries, selected_years, selected_countries)
    elif page == "Local Data Analysis":
        show_local_data_analysis(data_queries, selected_years)
    elif page == "Economic Insights":
        show_economic_insights(data_queries, selected_years, selected_countries)
    elif page == "ML Predictions":
        show_ml_predictions(data_queries, model_loader, selected_years, selected_countries)
    elif page == "Data Quality":
        show_data_quality(data_queries)

def show_overview(data_queries, selected_years):
    """Show overview dashboard"""
    st.header("📈 Data Overview")
    
    if not selected_years:
        st.warning("Please select at least one year from the sidebar.")
        return
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    try:
        # Get overview statistics
        wb_stats = data_queries.get_worldbank_overview(selected_years)
        local_stats = data_queries.get_local_data_overview(selected_years)
        
        with col1:
            st.metric(
                "Countries in Dataset",
                wb_stats.get('total_countries', 0),
                delta=None
            )
        
        with col2:
            st.metric(
                "World Bank Records",
                f"{wb_stats.get('total_records', 0):,}",
                delta=None
            )
        
        with col3:
            st.metric(
                "Local Data Records",
                f"{local_stats.get('total_records', 0):,}",
                delta=None
            )
        
        with col4:
            avg_gdp = wb_stats.get('avg_gdp', 0)
            st.metric(
                "Avg GDP (Billions)",
                f"${avg_gdp/1e9:.1f}B" if avg_gdp else "N/A",
                delta=None
            )
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("GDP Distribution by Year")
            gdp_by_year = data_queries.get_gdp_by_year(selected_years)
            if not gdp_by_year.empty:
                fig = px.box(
                    gdp_by_year, 
                    x='year', 
                    y='gdp_value',
                    title="GDP Distribution Across Countries"
                )
                fig.update_yaxis(title="GDP (Current USD)")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No GDP data available for selected years")
        
        with col2:
            st.subheader("Local Data Trends")
            local_trends = data_queries.get_local_data_trends(selected_years)
            if not local_trends.empty:
                fig = px.line(
                    local_trends,
                    x='year',
                    y='avg_value',
                    color='category',
                    title="Local Data Average Values by Category"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No local data available for selected years")
        
        # Recent data updates
        st.subheader("📅 Recent Data Updates")
        recent_updates = data_queries.get_recent_updates()
        if not recent_updates.empty:
            st.dataframe(recent_updates, use_container_width=True)
        else:
            st.info("No recent updates found")
            
    except Exception as e:
        st.error(f"Error loading overview data: {str(e)}")

def show_worldbank_analysis(data_queries, selected_years, selected_countries):
    """Show World Bank data analysis"""
    st.header("🌍 World Bank GDP Analysis")
    
    if not selected_years:
        st.warning("Please select at least one year from the sidebar.")
        return
    
    if not selected_countries:
        st.warning("Please select at least one country from the sidebar.")
        return
    
    try:
        # GDP Rankings
        st.subheader("🏆 GDP Rankings")
        gdp_rankings = data_queries.get_gdp_rankings(selected_years, selected_countries)
        
        if not gdp_rankings.empty:
            # Top 10 countries by latest year
            latest_year = max(selected_years)
            top_countries = gdp_rankings[gdp_rankings['year'] == latest_year].head(10)
            
            fig = px.bar(
                top_countries,
                x='country_name',
                y='gdp_value',
                title=f"Top 10 Countries by GDP - {latest_year}",
                color='gdp_value',
                color_continuous_scale='viridis'
            )
            fig.update_xaxis(tickangle=45)
            fig.update_yaxis(title="GDP (Current USD)")
            st.plotly_chart(fig, use_container_width=True)
            
            # GDP Growth Analysis
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📈 GDP Growth Rates")
                growth_data = data_queries.get_gdp_growth_analysis(selected_years, selected_countries)
                if not growth_data.empty:
                    fig = px.line(
                        growth_data,
                        x='year',
                        y='gdp_growth_rate',
                        color='country_name',
                        title="GDP Growth Rate Over Time"
                    )
                    fig.update_yaxis(title="Growth Rate (%)")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No growth data available")
            
            with col2:
                st.subheader("🌍 Regional Performance")
                regional_data = data_queries.get_regional_performance(selected_years)
                if not regional_data.empty:
                    fig = px.pie(
                        regional_data,
                        values='total_gdp',
                        names='region',
                        title="GDP Share by Region"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No regional data available")
            
            # Detailed data table
            st.subheader("📋 Detailed GDP Data")
            st.dataframe(
                gdp_rankings[['country_name', 'year', 'gdp_value', 'ranking', 'gdp_growth_rate']],
                use_container_width=True
            )
            
        else:
            st.info("No GDP data available for selected filters")
            
    except Exception as e:
        st.error(f"Error loading World Bank analysis: {str(e)}")

def show_local_data_analysis(data_queries, selected_years):
    """Show local data analysis"""
    st.header("📊 Local Data Analysis")
    
    if not selected_years:
        st.warning("Please select at least one year from the sidebar.")
        return
    
    try:
        # Local data overview
        local_summary = data_queries.get_local_data_summary(selected_years)
        
        if not local_summary.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                total_records = local_summary['total_records'].sum()
                st.metric("Total Records", f"{total_records:,}")
            
            with col2:
                avg_value1 = local_summary['avg_value_1'].mean()
                st.metric("Avg Value 1", f"{avg_value1:.2f}")
            
            with col3:
                categories = local_summary['category'].nunique()
                st.metric("Categories", categories)
            
            # Value distribution
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📈 Value Distribution Over Time")
                value_trends = data_queries.get_local_value_trends(selected_years)
                if not value_trends.empty:
                    fig = px.line(
                        value_trends,
                        x='year',
                        y=['avg_value_1', 'avg_value_2'],
                        title="Average Values Trend"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No trend data available")
            
            with col2:
                st.subheader("🎯 Category Performance")
                category_performance = data_queries.get_category_performance(selected_years)
                if not category_performance.empty:
                    fig = px.bar(
                        category_performance,
                        x='category',
                        y='total_value',
                        title="Total Value by Category"
                    )
                    fig.update_xaxis(tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No category data available")
            
            # Monthly patterns
            st.subheader("📅 Monthly Patterns")
            monthly_data = data_queries.get_monthly_patterns(selected_years)
            if not monthly_data.empty:
                fig = px.heatmap(
                    monthly_data.pivot(index='category', columns='month', values='avg_value'),
                    title="Monthly Average Values by Category"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No monthly pattern data available")
                
        else:
            st.info("No local data available for selected years")
            
    except Exception as e:
        st.error(f"Error loading local data analysis: {str(e)}")

def show_economic_insights(data_queries, selected_years, selected_countries):
    """Show economic insights and correlations"""
    st.header("💡 Economic Insights")
    
    if not selected_years or not selected_countries:
        st.warning("Please select years and countries from the sidebar.")
        return
    
    try:
        # Economic correlations
        st.subheader("🔗 Economic Correlations")
        correlation_data = data_queries.get_economic_correlations(selected_years, selected_countries)
        
        if not correlation_data.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("GDP vs Local Data Correlation")
                fig = px.scatter(
                    correlation_data,
                    x='local_avg_value',
                    y='gdp_value',
                    color='country_name',
                    size='year',
                    title="GDP vs Local Data Relationship"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Growth vs Performance")
                fig = px.scatter(
                    correlation_data,
                    x='gdp_growth_rate',
                    y='local_performance_score',
                    color='region',
                    title="GDP Growth vs Local Performance"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Economic indicators dashboard
            st.subheader("📊 Economic Indicators Dashboard")
            indicators = data_queries.get_economic_indicators(selected_years, selected_countries)
            
            if not indicators.empty:
                # Create subplot figure
                fig = make_subplots(
                    rows=2, cols=2,
                    subplot_titles=['GDP Volatility', 'Growth Consistency', 
                                   'Economic Stability', 'Performance Index'],
                    specs=[[{"secondary_y": False}, {"secondary_y": False}],
                           [{"secondary_y": False}, {"secondary_y": False}]]
                )
                
                # Add traces for different indicators
                countries_sample = indicators['country_name'].unique()[:5]
                for country in countries_sample:
                    country_data = indicators[indicators['country_name'] == country]
                    
                    fig.add_trace(
                        go.Scatter(x=country_data['year'], y=country_data['gdp_volatility'],
                                 name=country, legendgroup=country),
                        row=1, col=1
                    )
                
                fig.update_layout(height=600, title_text="Economic Indicators Analysis")
                st.plotly_chart(fig, use_container_width=True)
            
        else:
            st.info("No correlation data available for selected filters")
            
    except Exception as e:
        st.error(f"Error loading economic insights: {str(e)}")

def show_ml_predictions(data_queries, model_loader, selected_years, selected_countries):
    """Show ML model predictions"""
    st.header("🤖 ML Predictions")
    
    if not selected_years or not selected_countries:
        st.warning("Please select years and countries from the sidebar.")
        return
    
    try:
        # Model information
        st.subheader("📋 Model Information")
        model_info = model_loader.get_active_model_info()
        
        if model_info:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Model Type", model_info.get('model_type', 'N/A'))
            with col2:
                st.metric("Accuracy (R²)", f"{model_info.get('accuracy_score', 0):.4f}")
            with col3:
                st.metric("Training Date", model_info.get('training_date', 'N/A')[:10])
            with col4:
                st.metric("Version", model_info.get('model_version', 'N/A'))
            
            # Feature importance
            st.subheader("🎯 Feature Importance")
            feature_importance = model_loader.get_feature_importance()
            
            if feature_importance:
                importance_df = pd.DataFrame(feature_importance)
                fig = px.bar(
                    importance_df.head(10),
                    x='importance',
                    y='feature',
                    orientation='h',
                    title="Top 10 Feature Importances"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Predictions
            st.subheader("🔮 GDP Predictions")
            
            if st.button("Generate Predictions"):
                with st.spinner("Generating predictions..."):
                    predictions = model_loader.generate_predictions(selected_countries, selected_years)
                    
                    if predictions is not None and not predictions.empty:
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            # Actual vs Predicted
                            fig = px.scatter(
                                predictions,
                                x='actual_gdp',
                                y='predicted_gdp',
                                color='country_name',
                                title="Actual vs Predicted GDP"
                            )
                            # Add diagonal line for perfect prediction
                            min_val = min(predictions['actual_gdp'].min(), predictions['predicted_gdp'].min())
                            max_val = max(predictions['actual_gdp'].max(), predictions['predicted_gdp'].max())
                            fig.add_trace(
                                go.Scatter(
                                    x=[min_val, max_val],
                                    y=[min_val, max_val],
                                    mode='lines',
                                    name='Perfect Prediction',
                                    line=dict(dash='dash', color='red')
                                )
                            )
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with col2:
                            # Prediction errors
                            predictions['error'] = abs(predictions['actual_gdp'] - predictions['predicted_gdp'])
                            predictions['error_percentage'] = (predictions['error'] / predictions['actual_gdp']) * 100
                            
                            fig = px.bar(
                                predictions.head(10),
                                x='country_name',
                                y='error_percentage',
                                title="Prediction Error by Country (%)"
                            )
                            fig.update_xaxis(tickangle=45)
                            st.plotly_chart(fig, use_container_width=True)
                        
                        # Detailed predictions table
                        st.subheader("📊 Detailed Predictions")
                        display_predictions = predictions[['country_name', 'year', 'actual_gdp', 
                                                         'predicted_gdp', 'error_percentage']].round(2)
                        st.dataframe(display_predictions, use_container_width=True)
                        
                    else:
                        st.error("Failed to generate predictions")
            
        else:
            st.warning("No trained model available. Please run the model training DAG first.")
            
    except Exception as e:
        st.error(f"Error loading ML predictions: {str(e)}")

def show_data_quality(data_queries):
    """Show data quality metrics"""
    st.header("🔍 Data Quality Dashboard")
    
    try:
        # Data quality overview
        st.subheader("📊 Data Quality Overview")
        quality_metrics = data_queries.get_data_quality_metrics()
        
        if quality_metrics:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "World Bank Completeness",
                    f"{quality_metrics.get('wb_completeness', 0):.1f}%"
                )
            
            with col2:
                st.metric(
                    "Local Data Completeness",
                    f"{quality_metrics.get('local_completeness', 0):.1f}%"
                )
            
            with col3:
                st.metric(
                    "Duplicate Records",
                    quality_metrics.get('duplicates', 0)
                )
            
            with col4:
                st.metric(
                    "Data Freshness (Days)",
                    quality_metrics.get('data_age_days', 0)
                )
            
            # Data quality issues
            st.subheader("⚠️ Data Quality Issues")
            quality_issues = data_queries.get_data_quality_issues()
            
            if not quality_issues.empty:
                st.dataframe(quality_issues, use_container_width=True)
            else:
                st.success("No data quality issues found!")
            
            # Data lineage
            st.subheader("🔄 Data Lineage")
            lineage_info = data_queries.get_data_lineage()
            
            if lineage_info:
                st.json(lineage_info)
            else:
                st.info("Data lineage information not available")
                
        else:
            st.warning("Unable to calculate data quality metrics")
            
    except Exception as e:
        st.error(f"Error loading data quality dashboard: {str(e)}")

if __name__ == "__main__":
    main()