import pandas as pd
import pyodbc
from sqlalchemy import Column, Integer, String, MetaData, Date, Float
from sqlalchemy.engine import URL
import sqlalchemy as sal
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = 'users'

    user_id: Column[int] = Column(Integer, primary_key=True)
    first_name: Column[str] = Column(String)
    last_name: Column[str] = Column(String)
    sex: Column[Optional[str]] = Column(String)
    email: Column[Optional[str]] = Column(String)
    date_of_birth: Column[Optional[Date]] = Column(Date)
    job_title: Column[Optional[str]] = Column(String)
    age: Column[int] = Column(Integer)
    last_modified: Column[Optional[Date]] = Column(Date)

class Recipe(Base):
    __tablename__ = 'recipes'

    id: Column[int] = Column(Integer, primary_key=True)
    name: Column[str] = Column(String)
    minutes: Column[Optional[int]] = Column(Integer)
    submitted: Column[Optional[Date]] = Column(Date)
    tags: Column[Optional[str]] = Column(String)
    n_steps: Column[Optional[int]] = Column(Integer)
    description: Column[Optional[str]] = Column(String)
    ingredients: Column[Optional[str]] = Column(String)
    n_ingredients: Column[Optional[int]] = Column(Integer)
    complexity: Column[Optional[str]] = Column(String)

class Interaction(Base):
    __tablename__ = 'interactions'

    id: Column[int] = Column(Integer, primary_key=True)
    user_id: Column[int] = Column(Integer, ForeignKey('users.user_id'))
    recipe_id: Column[int] = Column(Integer, ForeignKey('recipes.id'))
    date: Column[Optional[Date]] = Column(Date)
    rating: Column[Optional[int]] = Column(Integer)
    review: Column[Optional[str]] = Column(String)
    rating_level: Column[Optional[str]] = Column(String)

    users = relationship('User', cascade='all, delete, delete-orphan')
    recipes = relationship('Recipe', cascade='all, delete, delete-orphan')


def users_etl(data):
    #Arreglar nombres de las colunmas
    data.columns = data.columns.str.replace(' ', '_')

    #Eliminar rows con datos vacíos
    data.dropna(inplace=True)

    #Eliminar datos duplicados
    data.drop_duplicates(inplace=True)

    # Ordena el DataFrame por la columna 'id' en orden ascendente
    data = data.sort_values(by='user_id')

    # Normalizar direcciones de correo electrónico a minúsculas
    data['email'] = data['email'].str.lower()

    # Calcular la edad a partir de la fecha de nacimiento
    data['age'] = pd.to_datetime('now').year - pd.to_datetime(data['date_of_birth']).dt.year

    # Agregar una columna de fecha de última modificación
    data['last_modified'] = pd.to_datetime('now')
    
    # Eliminar columnas innecesarias
    data = data.drop(columns=['phone', 'encoded_id'])

    return data

def recipes_etl(data):
    #Eliminar rows con datos vacíos
    data.dropna(inplace=True)

    #Eliminar datos duplicados
    data.drop_duplicates(inplace=True)
    
    # Eliminar columnas innecesarias
    data = data.drop(columns=['description', 'contributor_id', 'nutrition'])

    # Ordena el DataFrame por la columna 'id' en orden ascendente
    data = data.sort_values(by='id')

    # Definir una función de nivel de complejidad para mostrar la información de complejidad de la receta
    def complexity_level(minutes, n_steps):
        if minutes <= 15 and n_steps <= 5:
            return 'Easy'
        elif minutes <= 30 and n_steps <= 10:
            return 'Moderate'
        else:
            return 'Hard'

    # Aplica la función de nivel de complejidad para calcular la columna 'complexity'
    data['complexity'] = data.apply(lambda row: complexity_level(row['minutes'], row['n_ingredients']), axis=1)

    return data

def interactions_etl(data):
    #Eliminar rows con datos vacíos
    data.dropna(inplace=True)

    #Eliminar datos duplicados
    data.drop_duplicates(inplace=True)

    # Ordena el DataFrame por la columna 'id' en orden ascendente
    data = data.sort_values(by='user_id')

    # Función para clasificar los ratings en niveles
    def classify_rating(rating):
        if rating < 3:
            return 'Low'
        elif rating <= 4:
            return 'Medium'
        else:
            return 'High'

    # Aplica la función de clasificación para crear la columna 'rating_level'
    data['rating_level'] = data['rating'].apply(classify_rating)

    return data
 
if __name__ == '__main__':
 
    try:
        engine = sal.create_engine("mssql+pyodbc://sa:123456@SOL-LPT2579\SQLEXPRESS,1433/testSource?driver=ODBC+Driver+17+for+SQL+Server")
        metadata = MetaData()

        users_data = pd.read_csv('RAW_users.csv')
        recipes_data = pd.read_csv('RAW_recipes.csv')
        interactions_data = pd.read_csv('RAW_interactions.csv')

        users_data = users_etl(users_data)
        recipes_data = recipes_etl(recipes_data)
        interactions_data = interactions_etl(interactions_data)
        
        
        users_data.to_sql('users', engine, if_exists='append', index=False)
        recipes_data.to_sql('recipes', engine, if_exists='append', index=False)
        interactions_data.to_sql('interactions', engine, if_exists='append', index=False)

        Base.metadata.create_all(engine) 
    except Exception as ex:
        print("Something went wrong due to the following error: \n", ex)