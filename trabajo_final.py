# -*- coding: utf-8 -*-
"""
Created on Wed Jul 10 23:37:59 2024

@author: Yovi Coaquira
"""

#%%

#
## IMPORTACION DE LA DATA
import pandas as pd
import numpy as np

from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker



# extraemos los datos de Armenia
df_ar = pd.read_csv("C:/yovi/Armenia.csv")

# extraemos los datos de Cyprus
df_cy = pd.read_csv("C:/yovi/Cyprus.csv")

# extraemos los datos de India
df_in = pd.read_csv("C:/yovi/India.csv")

# extraemos los datos de Myanmar
df_my = pd.read_csv("C:/yovi/Myanmar.csv")

# extraemos los datos de Solomon Island
df_so = pd.read_csv("C:/yovi/Solomon Islands.csv")



#%%
# Pivot table
pvt_ar = df_ar.pivot_table(values='value', index='year', columns='indicator_code')
print(pvt_ar)

pvt_cy = df_cy.pivot_table(values='value', index='year', columns='indicator_code')
print(pvt_cy)

pvt_in = df_in.pivot_table(values='value', index='year', columns='indicator_code')
print(pvt_in)

pvt_my = df_my.pivot_table(values='value', index='year', columns='indicator_code')
print(pvt_my)

pvt_so = df_so.pivot_table(values='value', index='year', columns='indicator_code')
print(pvt_so)


#%%

## CALCULOS
# calcular IPS Armenia
aux1= (pvt_ar['NY.GDP.PCAP.KD']/pvt_ar['SP.POP.TOTL']) * (1- pvt_ar['SI.POV.GINI']) * (1- pvt_ar['SI.POV.LMIC.GP'])
print(aux1)
pvt_ar['IPS'] = round(aux1,2)
print(pvt_ar)

# calcular IPS Cyprus
aux2= (pvt_cy['NY.GDP.PCAP.KD']/pvt_cy['SP.POP.TOTL']) * (1- pvt_cy['SI.POV.GINI']) * (1- pvt_cy['SI.POV.LMIC.GP'])
print(aux2)
pvt_cy['IPS'] = round(aux2,2)
print(pvt_cy)

# calcular IPS India
aux3= (pvt_in['NY.GDP.PCAP.KD']/pvt_in['SP.POP.TOTL']) * (1- pvt_in['SI.POV.GINI']) * (1- pvt_in['SI.POV.LMIC.GP'])
print(aux3)
pvt_in['IPS'] = round(aux3,2)
print(pvt_in)

# calcular IPS Mynamar
aux4= (pvt_my['NY.GDP.PCAP.KD']/pvt_my['SP.POP.TOTL']) * (1- pvt_my['SI.POV.GINI']) * (1- pvt_my['SI.POV.LMIC.GP'])
print(aux4)
pvt_my['IPS'] = round(aux4,2)
print(pvt_my)

# calcular IPS Salomon Island
aux5= (pvt_so['NY.GDP.PCAP.KD']/pvt_so['SP.POP.TOTL']) * (1- pvt_so['SI.POV.GINI']) * (1- pvt_so['SI.POV.LMIC.GP'])
print(aux5)
pvt_so['IPS'] = round(aux5,2)
print(pvt_so)



#%% Connect to PostgreSQL
# DATABASE_URL = 'postgresql+psycopg2://username:password@hostname:port/dbname'

DATABASE_URL = "postgresql+psycopg2://utb_students:AVNS_OXQBajkVtAn2czuYQYe@pg-diplomado-utb-diplomado-utb-2024.c.aivencloud.com:24354/economic_kpis_utb"
engine = create_engine(DATABASE_URL)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Create a base class for the declarative model
Base = declarative_base()

# Define the yearly_values table
class YearlyValue(Base):
    __tablename__ = 'yearly_value'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    value = Column(Float, nullable=False)
    country_info_id = Column(Integer, ForeignKey('country_info.id'), nullable=False)
    indicator_id = Column(Integer, ForeignKey('indicator.id'), nullable=False)

    country_info = relationship("CountryInfo", back_populates="yearly_values")
    indicator = relationship("Indicator", back_populates="yearly_values")


#%% ARMENIA
ar= 42

#%%
new_yearly_value = YearlyValue(
    year=2022,
    value=0.20,
    country_info_id=42,
    indicator_id=39
)
session.add(new_yearly_value)
session.commit()

print("Data inserted successfully!")


# Armenia
for indice_fila, fila in pvt_ar.iterrows():
    new_yearly_value = YearlyValue(
        year=indice_fila,
        value=fila["IPS"],
        country_info_id=24,
        indicator_id=39
    )
    session.add(new_yearly_value)
    session.commit()

# Cyprus
for indice_fila, fila in pvt_cy.iterrows():
    new_yearly_value = YearlyValue(
        year=indice_fila,
        value=fila["IPS"],
        country_info_id=83,
        indicator_id=39
    )
    session.add(new_yearly_value)
    session.commit()

# India
for indice_fila, fila in pvt_in.iterrows():
    new_yearly_value = YearlyValue(
        year=indice_fila,
        value=fila["IPS"],
        country_info_id=124,
        indicator_id=39
    )
    session.add(new_yearly_value)
    session.commit()

# Myanmar
for indice_fila, fila in pvt_my.iterrows():
    new_yearly_value = YearlyValue(
        year=indice_fila,
        value=fila["IPS"],
        country_info_id=165,
        indicator_id=39
    )
    session.add(new_yearly_value)
    session.commit()

# Solomon Islands
for indice_fila, fila in pvt_so.iterrows():
    new_yearly_value = YearlyValue(
        year=indice_fila,
        value=fila["IPS"],
        country_info_id=206,
        indicator_id=39
    )
    session.add(new_yearly_value)
    session.commit()