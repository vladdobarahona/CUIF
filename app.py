# -*- coding: utf-8 -*-
"""
Aplicación Streamlit – Información Financiera CUIF (SFC)
-------------------------------------------------------

Author: Vladimir Alonso Barahona Palacios
Descripción:
Aplicación interactiva para consultar, descargar y procesar
información financiera CUIF (Superintendencia Financiera de Colombia),
integrar con plantilla NIIF y generar reporte Excel estructurado.
"""

import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from io import BytesIO
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
import re

# ==============================
# CONFIGURACIÓN GENERAL
# ==============================
BASE_URL = "https://www.datos.gov.co/resource/mxk5-ce6w.json"
LIMIT = 50000  # paginación

# FACTORES DE CONVERSIÓN
UNIDADES_VALOR = {
    "Miles": (1_000, "Miles de Pesos"),
    "Millones": (1_000_000, "Millones de Pesos"),
    "Cientos de Millones": (100_000_000, "Cientos de Millones de Pesos"),
    "Miles de Millones": (1_000_000_000, "Miles de Millones de Pesos"),
    "Billones": (1_000_000_000_000, "Billones de Pesos"),
}

# ==============================
# FUNCIONES
# ==============================

def max_fecha():
    """Consulta la fecha máxima en el dataset CUIF."""
    query = '''
    https://www.datos.gov.co/resource/mxk5-ce6w.json?$query=
    SELECT max(fecha_corte)
    '''
    r = requests.get(query)
    if r.status_code != 200:
        return None
    data = r.json()
    return data[0]["max_fecha_corte"] if data else None


def conteo(tipo_entidad: str, fecha_desde: str, fecha_hasta: str) -> int:
    """Cuenta registros según filtros."""
    where = (
        f"fecha_corte between '{fecha_desde}T00:00:00' and '{fecha_hasta}T23:59:59'"
        f" AND nombre_moneda = 'Total'"
        f" AND nombre_tipo_entidad = '{tipo_entidad}'"
    )
    params = {"$select": "count(*)", "$where": where}
    r = requests.get(BASE_URL, params=params)
    if r.status_code != 200:
        raise Exception(f"Error HTTP {r.status_code}: {r.text}")
    data = r.json()
    return int(data[0].get("count", 0)) if data else 0


def descargar_datos(tipo_entidad, fecha_desde, fecha_hasta):
    """Descarga datos con paginación."""
    where_clause = (
        f"fecha_corte between '{fecha_desde}T00:00:00' and '{fecha_hasta}T23:59:59'"
        f" AND nombre_moneda = 'Total'"
        f" AND nombre_tipo_entidad = '{tipo_entidad}'"
    )

    offset = 0
    all_rows = []

    while True:
        params = {"$limit": LIMIT, "$offset": offset, "$where": where_clause}
        r = requests.get(BASE_URL, params=params)

        if r.status_code != 200:
            raise Exception(f"Error HTTP {r.status_code}: {r.text}")

        data = r.json()
        if not data:
            break

        all_rows.extend(data)
        offset += LIMIT

    return pd.DataFrame(all_rows)


def procesar_dataframe(df, plantilla_path, factor_rango):
    df_final = df.copy()

    df_final["valor_Rango_de_Valores"] = (
        pd.to_numeric(df_final["valor"], errors="coerce") / factor_rango
    ).round(0)

    df_final["Label"] = df_final["codigo_entidad"].astype(str) + " - " + df_final["nombre_entidad"]

    pivot_df = pd.pivot(
        df_final,
        index=["cuenta", "nombre_cuenta"],
        columns="Label",
        values="valor_Rango_de_Valores",
    )

    pivot_df.columns.name = None
    pivot_df = pivot_df.reset_index()

    # Ordenar columnas
    def sort_by_prefix(col):
        match = re.match(r'^(\d+)\s*-', str(col))
        return int(match.group(1)) if match else -1

    fixed = ["cuenta", "nombre_cuenta"]
    other_cols = [c for c in pivot_df.columns if c not in fixed]
    pivot_df = pivot_df[fixed + sorted(other_cols, key=sort_by_prefix)]

    # MERGE CON PLANTILLA
    plantilla = pd.read_excel(plantilla_path, sheet_name="Cuentas")
    plantilla = plantilla[["Cuenta", "Descripción_Cuenta"]]

    plantilla["Cuenta"] = plantilla["Cuenta"].astype(str)
    pivot_df["cuenta"] = pivot_df["cuenta"].astype(str)

    plantilla = plantilla.rename(columns={"Cuenta": "cuenta", "Descripción_Cuenta": "nombre_cuenta"})
    plantilla = plantilla.set_index(["cuenta"])
    pivot_df = pivot_df.set_index(["cuenta"])

    pivot_full = plantilla.join(pivot_df, how="left").fillna(0)
    pivot_full = pivot_full.reset_index()

    return pivot_full


def generar_excel(pivot_df, tipo_entidad, fecha_desde, factor_rango, etiqueta_unidad):
    date_obj = datetime.strptime(fecha_desde, "%Y-%m-%d")
    f_informe = date_obj.strftime("%d/%m/%Y")
    formatted_date = date_obj.strftime("%d%m%Y")

    wb = Workbook()
    ws = wb.active
    ws.title = f"00{formatted_date}g1m0cie"

    # ENCABEZADOS
    ws["A2"] = "Tipo de Entidad:"
    ws["B2"] = tipo_entidad

    ws["A3"] = "Fecha de Informe:"
    ws["B3"] = f_informe

    ws["A4"] = "Moneda:"
    ws["B4"] = "0 Total"

    ws["A5"] = "Rango de Valores:"
    ws["B5"] = str(factor_rango)
    ws["C5"] = etiqueta_unidad

    # MATRIZ
    start_row = 9
    for r_idx, row in enumerate(dataframe_to_rows(pivot_df, index=False, header=True)):
        for c_idx, value in enumerate(row):
            ws[f"{get_column_letter(1 + c_idx)}{start_row + r_idx}"] = value

    output = BytesIO()
    wb.save(output)
    return output.getvalue()

# ==============================
# INTERFAZ STREAMLIT
# ==============================
st.title("📊 CUIF – Descarga y Procesamiento de Información Financiera")

# Fecha máxima
if st.button("Consultar Fecha Máxima Disponible"):
    fecha = max_fecha()
    if fecha:
        st.success(f"📅 Fecha máxima encontrada: **{fecha}**")
    else:
        st.error("No se pudo obtener la fecha máxima.")

st.subheader("Seleccione el rango de fechas")
fecha_desde = st.date_input("Fecha Desde")
fecha_hasta = st.date_input("Fecha Hasta")

# TIPOS DE ENTIDAD
lista_tipo_entidad = [
    "ESTABLECIMIENTOS BANCARIOS",
    "COMPANIAS DE SEGUROS GENERALES",
    "COMPANIAS DE SEGUROS DE VIDA",
    "SOCIEDADES FIDUCIARIAS",
]

tipo_entidad = st.selectbox("Tipo de entidad (SFC):", lista_tipo_entidad)

# RANGO DE VALORES (Miles, Millones, Billones...)
st.subheader("Rango de Valores (Unidad de salida)")
opcion_rango = st.selectbox("Seleccione unidad:", list(UNIDADES_VALOR.keys()))
factor_rango, etiqueta_unidad = UNIDADES_VALOR[opcion_rango]

plantilla_file = st.file_uploader("Subir plantilla de cuentas NIIF", type=["xlsx"])

# BOTÓN FINAL
if st.button("Validar y Descargar"):

    if fecha_desde > fecha_hasta:
        st.error("⚠ La fecha inicial no puede ser mayor a la final.")
    elif plantilla_file is None:
        st.error("⚠ Debe subir la plantilla NIIF.")
    else:
        fecha_desde_str = fecha_desde.strftime("%Y-%m-%d")
        fecha_hasta_str = fecha_hasta.strftime("%Y-%m-%d")

        st.info("🔍 Consultando cantidad de registros…")
        cantidad = conteo(tipo_entidad, fecha_desde_str, fecha_hasta_str)
        st.write(f"📌 Registros encontrados: **{cantidad:,}**")

        st.info("⬇️ Descargando datos…")
        df = descargar_datos(tipo_entidad, fecha_desde_str, fecha_hasta_str)

        st.info("🔧 Procesando datos…")
        pivot_df = procesar_dataframe(df, plantilla_file, factor_rango)

        st.success("Archivo generado correctamente 🎉")

        xlsx_bytes = generar_excel(
            pivot_df, tipo_entidad, fecha_desde_str, factor_rango, etiqueta_unidad
        )

        st.download_button(
            "📥 Descargar archivo XLSX",
            data=xlsx_bytes,
            file_name=f"00{fecha_desde.strftime('%d%m%Y')}n.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
