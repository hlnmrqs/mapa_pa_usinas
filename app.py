import streamlit as st
import pandas as pd
import requests
import folium
from folium.plugins import HeatMap, MarkerCluster
from streamlit_folium import st_folium
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential

# ============================================
# Configurações Iniciais
# ============================================
st.set_page_config(
    page_title="Mapa de Usinas Solares", 
    layout="wide",
    page_icon="☀️",
    initial_sidebar_state="expanded"
)

# ============================================
# Classe Config
# ============================================
class Config:
    CACHE_DIR = Path('.cache')
    DATA_DIR = Path('data')
    FRANCHISE_INPUT = Path('input/franquias.csv')
    USINA_INPUT = Path('input/dados_com_geolocalizacao.csv') 

    # Arquivos (usando parquet para performance)
    ANEEL_CACHE = CACHE_DIR / 'aneel.parquet'
    COMPANY_DATA = DATA_DIR / 'empresa.parquet'
    FRANCHISE_DATA = DATA_DIR / 'franquias.parquet'
    
    # Mapa
    MAP_CENTER = [-14.235, -51.9253]
    MAP_ZOOM = 4
    MAP_TILES = 'CartoDB positron'
    
    # API ANEEL
    ANEEL_URL = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search_sql"
    ANEEL_QUERY = """
    SELECT "NomMunicipio", "SigUF", "NomRegiao",
           "NumCoordNEmpreendimento" AS lat_str,
           "NumCoordEEmpreendimento" AS lng_str
    FROM "b1bd71e7-d0ad-4214-9053-cbd58e9564a7"
    WHERE 
      "SigUF" = 'PA'
      AND "NumCoordNEmpreendimento" IS NOT NULL
    LIMIT 80000
    """

    @classmethod
    def init_dirs(cls):
        """Cria diretórios necessários"""
        cls.CACHE_DIR.mkdir(exist_ok=True, parents=True)
        cls.DATA_DIR.mkdir(exist_ok=True, parents=True)

# ============================================
# Funções de Carregamento
# ============================================
@st.cache_resource(ttl=3600*24)
def init_data():
    """Inicializa todos os dados uma única vez"""
    return {
        'aneel': load_aneel_data(),
        'franchise': load_franchise_data(),
        'usinas': load_usina_data()
    }

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_aneel():
    """Busca dados da ANEEL com tratamento robusto"""
    try:
        response = requests.get(
            Config.ANEEL_URL,
            params={'sql': Config.ANEEL_QUERY},
            timeout=10
        )
        response.raise_for_status()
        return pd.DataFrame(response.json().get('result', {}).get('records', []))
    except requests.exceptions.RequestException as e:
        st.error(f"Erro na API ANEEL: {str(e)}")
        return pd.DataFrame()

def load_aneel_data():
    """Carrega dados ANEEL com cache inteligente"""
    if Config.ANEEL_CACHE.exists():
        try:
            return pd.read_parquet(Config.ANEEL_CACHE)
        except Exception as e:
            st.warning(f"Cache ANEEL inválido, recriando...: {str(e)}")
    
    df = fetch_aneel()
    if not df.empty:
        df = process_coordinates(df)
        df.to_parquet(Config.ANEEL_CACHE)
    else:
        st.error("Dados da ANEEL estão vazios.")
    return df

def load_franchise_data():
    """Carrega dados de franquias do CSV e converte para parquet"""
    if Config.FRANCHISE_DATA.exists():
        try:
            return pd.read_parquet(Config.FRANCHISE_DATA)
        except Exception as e:
            st.error(f"Erro ao carregar dados de franquias: {str(e)}")
    
    if Config.FRANCHISE_INPUT.exists():
        try:
            df = pd.read_csv(Config.FRANCHISE_INPUT)
            df = df.dropna(subset=['latitude', 'longitude'])  # Certifique-se de que essas colunas existem
            df.to_parquet(Config.FRANCHISE_DATA)
            return df
        except Exception as e:
            st.error(f"Erro ao carregar franquias do CSV: {str(e)}")
    
    return pd.DataFrame()

def filter_usinas_by_region(df):
    """Filtra usinas com base nas coordenadas para as regiões Norte e Nordeste"""
    # Definindo os limites de latitude e longitude
    lat_norte = (5.0, -5.0)
    long_norte = (-75.0, -60.0)

    lat_nordeste = (-5.0, -18.0)
    long_nordeste = (-75.0, -34.0)

    # Filtrando os dados
    filtered_df = df[
        ((df['latitude'].between(lat_norte[1], lat_norte[0])) & (df['longitude'].between(long_norte[0], long_norte[1]))) |
        ((df['latitude'].between(lat_nordeste[1], lat_nordeste[0])) & (df['longitude'].between(long_nordeste[0], long_nordeste[1])))
    ]
    
    return filtered_df

@st.cache_data(ttl=3600*24)  # Cache por 24 horas
def load_usina_data():
    """Carrega dados das usinas do CSV e converte para parquet"""
    if Config.USINA_INPUT.exists():
        try:
            df = pd.read_csv(Config.USINA_INPUT)
            df = df.dropna(subset=['latitude', 'longitude'])
            # Filtra usinas para as regiões Norte e Nordeste
            df = filter_usinas_by_region(df)
            return df
        except Exception as e:
            st.error(f"Erro ao carregar usinas do CSV: {str(e)}")
    
    st.error("Arquivo de usinas não encontrado.")
    return pd.DataFrame()

def process_coordinates(df):
    """Processa coordenadas de forma eficiente"""
    df['latitude'] = pd.to_numeric(df['lat_str'].str.replace(',', '.'), errors='coerce')
    df['longitude'] = pd.to_numeric(df['lng_str'].str.replace(',', '.'), errors='coerce')
    return df.dropna(subset=['latitude', 'longitude']).copy()

def add_franchise_markers(mapa, data):
    """Adiciona marcadores das franquias ao mapa"""
    if not data.empty:
        cluster = MarkerCluster(name="Franquias")
        for _, row in data.iterrows():
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=row.get('Franquia', 'N/A'),  # Nome da franquia
                icon=folium.Icon(color="orange")
            ).add_to(cluster)
        cluster.add_to(mapa)

def add_usina_heatmap(mapa, data):
    """Adiciona heatmap das usinas ao mapa"""
    add_heatmap(mapa, data, "Usinas", radius=10, gradient={0.0: 'yellow', 0.5: 'orange', 1.0: 'red'})  # Gradiente amarelo

def load_company_data():
    """Carrega dados da empresa"""
    if Config.COMPANY_DATA.exists():
        try:
            return pd.read_parquet(Config.COMPANY_DATA)
        except Exception as e:
            st.error(f"Erro nos dados da empresa: {str(e)}")
    return pd.DataFrame()

# ============================================
# Função para Adicionar Heatmap
# ============================================
def add_heatmap(mapa, data, name, radius=10, gradient=None):
    """Adiciona heatmap genérico"""
    if not data.empty:
        HeatMap(
            data[['latitude', 'longitude']].values,
            radius=radius,
            name=name,
            gradient=gradient  # Usa o gradiente passado como argumento
        ).add_to(mapa)

# ============================================
# Visualização
# ============================================
def create_base_map():
    """Cria mapa base leve"""
    return folium.Map(
        location=Config.MAP_CENTER,
        zoom_start=Config.MAP_ZOOM,
        tiles=Config.MAP_TILES,
        prefer_canvas=True,  # Melhora performance
        control_scale=True
    )

# ============================================
# Interface do Usuário
# ============================================
def create_sidebar():
    """Cria a barra lateral com controles"""
    with st.sidebar:
        st.title("⚙️ Controles")
        
        with st.expander("🌐 Camadas", expanded=True):
            layers = {
                'aneel': st.checkbox("Usinas ANEEL", True, key='aneel'),
                'franchise': st.checkbox("Franquias", True, key='franchise'),
                'usinas': st.checkbox("Usinas Solares", True, key='usinas')
            }
        
        with st.expander("📈 Estatísticas", expanded=True):
            if 'data' in st.session_state:
                cols = st.columns(3)
                cols[0].metric("ANEEL", len(st.session_state.data['aneel']))
                cols[1].metric("Nossas Usinas", len(st.session_state.data['usinas']))
                cols[2].metric("Franquias", len(st.session_state.data['franchise']))
                
        
        st.markdown("---")
        st.caption(f"🔄 Atualizado em: {pd.Timestamp.now().strftime('%d/%m %H:%M')}")

    return layers

# ============================================
# Aplicação Principal
# ============================================
def main():
    # Inicialização
    Config.init_dirs()
    
    # Título e descrição
    st.title("🌞 Mapa de Usinas Solares")
    st.markdown("Visualização interativa de usinas solares no Norte/Nordeste")
    
    # Carrega dados (apenas uma vez)
    if 'data' not in st.session_state:
        with st.spinner("🚀 Carregando dados..."):
            st.session_state.data = init_data()

    if 'usinas' not in st.session_state.data:
        st.error("Dados de usinas não foram carregados corretamente.")
        return

    # Franquias
    franchise_data = load_franchise_data()

    # Sidebar
    layers = create_sidebar()
    
    # Mapa principal
    with st.container():
        col1, col2 = st.columns([3, 1])
        
        with col1:
            mapa = create_base_map()
            
            if layers['aneel']:
                # Adiciona heatmap da ANEEL com gradiente azul
                add_heatmap(mapa, st.session_state.data['aneel'], "ANEEL", radius=8, gradient={0.0: 'blue', 0.5: 'lightblue', 1.0: 'cyan'})

            if layers['franchise']:
                add_franchise_markers(mapa, franchise_data)

            if layers['usinas']:  # Nova camada
                # Adiciona heatmap das usinas com gradiente amarelo
                add_usina_heatmap(mapa, st.session_state.data['usinas'])
            
            folium.LayerControl().add_to(mapa)

            st_folium(
                mapa,
                width=800,
                height=600,
                returned_objects=[],
                key='performance_map'
            )
        
        with col2:
            st.markdown("### 📍 Legenda")
            st.markdown("- 🔵 **ANEEL**: Usinas registradas")
            st.markdown("- 🟢 **Nossas Usinas**: Projetos ativos")
            st.markdown("- 🟠 **Franquias**: Parceiros comerciais")
            st.markdown("---")
            st.info("Use os controles à esquerda para filtrar as camadas visíveis.")

if __name__ == "__main__":
    main()
