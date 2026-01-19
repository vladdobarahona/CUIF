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
# CONFIG
# ==============================
BASE_URL = "https://www.datos.gov.co/resource/mxk5-ce6w.json"
LIMIT = 50000

# ==============================
# RANGOS DE VALORES
# ==============================
UNIDADES_VALOR = {
    "Sin Unidades": (1, "Pesos"),
    "Miles": (1_000, "Miles de Pesos"),
    "Millones": (1_000_000, "Millones de Pesos"),
    "Cientos de Millones": (100_000_000, "Cientos de Millones de Pesos"),
    "Miles de Millones": (1_000_000_000, "Miles de Millones de Pesos"),
    "Billones": (1_000_000_000_000, "Billones de Pesos"),
}

# ==============================
# FUNCIONES API
# ==============================
def max_fecha():
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

# ==============================
# PROCESAMIENTO
# ==============================
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

    # Orden columnas
    def sort_by_prefix(col):
        match = re.match(r'^(\d+)\s*-', str(col))
        return int(match.group(1)) if match else 99999

    fixed = ["cuenta", "nombre_cuenta"]
    others = [c for c in pivot_df.columns if c not in fixed]
    pivot_df = pivot_df[fixed + sorted(others, key=sort_by_prefix)]

    # MERGE PLANTILLA
    plantilla = pd.read_excel(plantilla_path, sheet_name="Cuentas")
    plantilla = plantilla[["Cuenta", "Descripción_Cuenta"]]
    plantilla["Cuenta"] = plantilla["Cuenta"].astype(str)
    pivot_df["cuenta"] = pivot_df["cuenta"].astype(str)

    plantilla = plantilla.rename(columns={"Cuenta": "cuenta", "Descripción_Cuenta": "nombre_cuenta"})
    plantilla = plantilla.set_index("cuenta")
    pivot_df = pivot_df.set_index("cuenta")

    pivot_full = plantilla.join(pivot_df, how="left").fillna(0).reset_index()

    return pivot_full

# ==============================
# GENERAR EXCEL
# ==============================
def generar_excel(pivot_df, tipo_entidad, fecha_desde, factor_rango, etiqueta_unidad):
    date_obj = datetime.strptime(fecha_desde, "%Y-%m-%d")
    f_informe = date_obj.strftime("%d/%m/%Y")
    formatted_date = date_obj.strftime("%d%m%Y")

    wb = Workbook()
    ws = wb.active
    ws.title = f"00{formatted_date}g1m0cie"

    ws["A2"] = "Tipo de Entidad:"
    ws["B2"] = tipo_entidad

    ws["A3"] = "Fecha de Informe:"
    ws["B3"] = f_informe

    ws["A4"] = "Moneda:"
    ws["B4"] = "0 Total"

    ws["A5"] = "Rango de Valores:"
    ws["B5"] = str(factor_rango)
    ws["C5"] = etiqueta_unidad

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

# Fecha máxima disponible
if st.button("Consultar Fecha Máxima Disponible"):
    fecha = max_fecha()
    st.success(f"📅 Fecha máxima encontrada: **{fecha}**") if fecha else st.error("Error consultando fecha.")

# Entradas obligatorias
st.subheader("Rango de fechas")
fecha_desde = st.date_input("Desde")
fecha_hasta = st.date_input("Hasta")

# Tipo entidad
lista_tipo_entidad = [
    "ESTABLECIMIENTOS BANCARIOS",
    "COMPANIAS DE SEGUROS GENERALES",
    "COMPANIAS DE SEGUROS DE VIDA",
    "SOCIEDADES FIDUCIARIAS",
]

tipo_entidad = st.selectbox("Tipo de Entidad (SFC):", lista_tipo_entidad)

# Nuevo: Selección de unidad
st.subheader("Rango de Valores")
opcion_rango = st.selectbox("Unidad:", list(UNIDADES_VALOR.keys()))
factor_rango, etiqueta_unidad = UNIDADES_VALOR[opcion_rango]
st.caption(f"Dividir por **{factor_rango:,}** ({etiqueta_unidad})")

# Plantilla NIIF
plantilla_file = st.file_uploader("Suba plantilla NIIF", type=["xlsx"])

# BOTÓN PRINCIPAL
if st.button("Validar y Descargar"):
    if fecha_desde > fecha_hasta:
        st.error("⚠ La fecha inicial no puede ser mayor que la final.")
    elif plantilla_file is None:
        st.error("⚠ Debe cargar la plantilla NIIF.")
    else:
        fecha_desde_str = fecha_desde.strftime("%Y-%m-%d")
        fecha_hasta_str = fecha_hasta.strftime("%Y-%m-%d")

        st.info("🔍 Consultando cantidad...")
        total = conteo(tipo_entidad, fecha_desde_str, fecha_hasta_str)
        st.write(f"Registros encontrados: **{total:,}**")

        st.info("⬇ Descargando información...")
        df = descargar_datos(tipo_entidad, fecha_desde_str, fecha_hasta_str)

        st.info("🔧 Procesando...")
        pivot_df = procesar_dataframe(df, plantilla_file, factor_rango)

        st.success("Archivo generado correctamente 🎉")

        xlsx = generar_excel(pivot_df, tipo_entidad, fecha_desde_str, factor_rango, etiqueta_unidad)

        st.download_button(
            "📥 Descargar Excel",
            data=xlsx,
            file_name=f"00{fecha_desde.strftime('%d%m%Y')}n.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
