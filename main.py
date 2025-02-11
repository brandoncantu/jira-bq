import os
import tempfile
import json
from datetime import datetime, timedelta
from jira import JIRA
from google.cloud import bigquery
from google.oauth2 import service_account
import time

# Jira connection details
JIRA_SERVER = "https://domain.atlassian.net/"
JIRA_USERNAME = "bcantul@domain.com"
JIRA_API_TOKEN = "APIKEY###############"
# BigQuery details
PROJECT_ID = "project-id"
DATASET_ID = "dataset_id"
TABLE_ID = "table_id"

CUSTOM_MAPPING_PATH = 'C:/Users/USER/Pictures/Code/jira_tickets/custom_mapping.json'
SCHEMA_PATH = 'C:/Users/USER/Pictures/Code/jira_tickets/schema.json'
CREDENTIALS_PATH = "C:/Users/USER/Downloads/sa-key.json"


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def map_field_name(field_id, field_map, custom_field_mapping):
    jira_name = field_map.get(field_id, field_id)
    return custom_field_mapping.get(jira_name, jira_name)


def create_row(issue, reverse_mapping, custom_field_mapping, bigquery_columns, schema_json, field_map):
    flat_issue = flatten_dict(issue.raw['fields'])
    mapped_issue = {map_field_name(k, field_map, custom_field_mapping): v for k, v in flat_issue.items()}
    mapped_issue[custom_field_mapping.get('Key', 'Key')] = issue.key
    mapped_issue[custom_field_mapping.get('id', 'id')] = issue.id

    row = {}
    for column in bigquery_columns:
        jira_field = reverse_mapping.get(column, column)
        value = mapped_issue.get(column)
        if schema_json[column] == "TIMESTAMP" and value is not None:
            row[column] = str(datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z"))
        elif value is None:
            row[column] = None
        else:
            row[column] = str(value)
    return row


def connect_to_jira():
    """Establish a connection to Jira and return the client."""
    return JIRA(server=JIRA_SERVER, basic_auth=(JIRA_USERNAME, JIRA_API_TOKEN))


def fetch_jira_issues(jira, start_date, end_date):
    """Fetch Jira issues within the specified date range."""
    print("Starting fetch from jira...")
    print(start_date)
    print(end_date)
    ###jql_query = f'updated >= "{start_date.strftime("%Y-%m-%d")}" AND updated <= "{end_date.strftime("%Y-%m-%d")}"'
    jql_query = f'updated >= "{start_date.strftime("%Y-%m-%d")}"'
    return jira.search_issues(jql_query, maxResults=False, expand='changelog')


def connect_to_bigquery():
    """Establish a connection to BigQuery and return the client."""
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def convert_to_type(value, field_type):
    if value is None:
        return None
    if field_type == 'INTEGER':
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    elif field_type == 'FLOAT':
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    elif field_type == 'BOOLEAN':
        return value.lower() in ('true', '1', 't', 'y', 'yes') if isinstance(value, str) else bool(value)
    elif field_type == 'TIMESTAMP':
        # You might want to add more sophisticated datetime parsing here
        return value
    else:
        return str(value)

def clean_data(rows, schema):
    cleaned_rows = []
    for row in rows:
        cleaned_row = {}
        for field in schema:
            if field.name in row:
                cleaned_row[field.name] = convert_to_type(row[field.name], field.field_type)
        cleaned_rows.append(cleaned_row)
    return cleaned_rows

def upload_to_bigquery(client, rows_to_insert, schema):
    """Upload data to BigQuery using a merge operation."""
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    try:
        table = client.get_table(table_ref)
    except Exception:
        print("Creating table...")
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)

    # Clean the data
    cleaned_rows = clean_data(rows_to_insert, schema)

    # Create a temporary table to hold the new data
    temp_table_id = f"{TABLE_ID}_temp_{int(time.time())}"
    temp_table_ref = client.dataset(DATASET_ID).table(temp_table_id)
    temp_table = bigquery.Table(temp_table_ref, schema=schema)
    temp_table = client.create_table(temp_table)

    # Configure the job to load data into the temporary table
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Start the job to load data into the temporary table
    job = client.load_table_from_json(cleaned_rows, temp_table_ref, job_config=job_config)

    # Wait for the job to complete
    job.result()

    print(f"Loaded {job.output_rows} rows into {temp_table_id}")

    # Perform the merge operation
    merge_query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
    USING `{PROJECT_ID}.{DATASET_ID}.{temp_table_id}` S
    ON T.Key = S.Key
    WHEN MATCHED THEN
      UPDATE SET {', '.join([f"{field.name} = S.{field.name}" for field in schema if field.name != 'Key'])}
    WHEN NOT MATCHED THEN
      INSERT ({', '.join([field.name for field in schema])})
      VALUES ({', '.join([f'S.{field.name}' for field in schema])})
    """

    query_job = client.query(merge_query)
    query_job.result()  # Wait for the job to complete

    # Clean up the temporary table
    client.delete_table(temp_table_ref)

    print(f"Successfully merged {len(cleaned_rows)} Jira issues into BigQuery")
    return True

'''def upload_to_bigquery(client, rows_to_insert, schema):
    """Upload data to BigQuery using a merge operation."""
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    try:
        table = client.get_table(table_ref)
    except Exception:
        print("Creating table...")
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)

    # Create a temporary table to hold the new data
    temp_table_id = f"{TABLE_ID}_temp_{int(time.time())}"
    temp_table_ref = client.dataset(DATASET_ID).table(temp_table_id)
    temp_table = bigquery.Table(temp_table_ref, schema=schema)
    temp_table = client.create_table(temp_table)

    # Insert rows into the temporary table
    print("Creating temporary table...")
    errors = client.insert_rows_json(temp_table, rows_to_insert)
    if errors:
        print("Errors occurred while inserting rows into temporary table:")
        for error in errors:
            print(error)
        client.delete_table(temp_table_ref)
        return False

    # Perform the merge operation
    print("Starting merge...")
    merge_query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
    USING `{PROJECT_ID}.{DATASET_ID}.{temp_table_id}` S
    ON T.Key = S.Key
    WHEN MATCHED THEN
      UPDATE SET {', '.join([f"{field.name} = S.{field.name}" for field in schema if field.name != 'Key'])}
    WHEN NOT MATCHED THEN
      INSERT ({', '.join([field.name for field in schema])})
      VALUES ({', '.join([f'S.{field.name}' for field in schema])})
    """

    query_job = client.query(merge_query)
    query_job.result()  # Wait for the job to complete

    # Clean up the temporary table
    client.delete_table(temp_table_ref)

    print(f"Successfully merged {len(rows_to_insert)} Jira issues into BigQuery")
    return True
'''

def prepare_data_for_bigquery(jira, issues, custom_field_mapping, schema_json, bigquery_columns):
    """Prepare Jira data for BigQuery insertion."""
    all_fields = jira.fields()
    field_map = {field['id']: field['name'] for field in all_fields}
    reverse_mapping = {v: k for k, v in custom_field_mapping.items()}

    rows_to_insert = []
    field_names = set()
    schema = []

    for i, issue in enumerate(issues):
        print(f'Processing issue {i + 1}/{len(issues)}')
        full_issue = jira.issue(issue.key, expand='changelog')
        ##DEBUG
        flat_issue = flatten_dict(issue.raw['fields'])
        print(flat_issue)
        break
        row = create_row(full_issue, reverse_mapping, custom_field_mapping, bigquery_columns, schema_json, field_map)

        for field_name, field_value in row.items():
            if field_name not in field_names:
                field_names.add(field_name)
                field_type = schema_json[field_name]
                schema.append(bigquery.SchemaField(field_name, field_type))

        rows_to_insert.append(row)

    return rows_to_insert, schema


def main():
    # Load configurations
    custom_field_mapping = load_json(CUSTOM_MAPPING_PATH)
    schema_json = load_json(SCHEMA_PATH)
    bigquery_columns = ["Issue_Type",
                        "Key",
                        "Summary",
                        "Assignee",
                        "Reporter",
                        "Priority",
                        "Status",
                        "Resolution",
                        "Created",
                        "Updated",
                        "Due_date",
                        "Time_to_resolution",
                        "CHART_Date_of_First_Response",
                        "A_que_ubicacion_y_a_que_area_corresponde",
                        "Account_vulnerability_type",
                        "Accountable",
                        "Actual_problem_MCEC",
                        "Afecta_otro_sistema_MCEC",
                        "Affected_hardware",
                        "Affected_router",
                        "Affected_services",
                        "Affects_versions",
                        "Ahorro_proyectado",
                        "Ambiente",
                        "Analisis_del_cambio",
                        "Change_Analysis",
                        "Aplicaciones_a_usar",
                        "Aplicativo",
                        "Approver_groups",
                        "Approvers",
                        "Approvers_migrated",
                        "Categorias_de_Cierre_Negocios_y_administrativos",
                        "CCB_comments",
                        "Categorias_de_Cierre_Matriz_de_Abastos",
                        "Cell_ITSM_Asset",
                        "Cells_ITSM",
                        "Categorias_de_cierre_Finanzas",
                        "Categorias_de_Cierre_Abastecimientos",
                        "Categoria_del_Servicio_ESM",
                        "Categoria",
                        "Category_Service_Request_and_Change",
                        "Category_Incident_and_Service_Request",
                        "Category_1",
                        "Category_2",
                        "Caso_de_uso_WTI",
                        "Caso_de_uso_1",
                        "Caso_de_uso_2",
                        "Case_type_1",
                        "Case_type_2",
                        "Cancellation_reason",
                        "Cambio_a_habilitar",
                        "Calificacion_Satisfaccion_del_Servicio",
                        "Calificacion_Calidad_del_Servicio",
                        "CAB_Approvers",
                        "Business_Unit_que_hace_la_solicitud",
                        "Business_Unit_2023",
                        "Business_Unit_Location",
                        "Business_Unit_Assets",
                        "Business_Unit",
                        "Business_Systems_Assets",
                        "Business_Systems",
                        "Business_capacity",
                        "Budget",
                        "Bold_Move",
                        "Change_reason_migrated",
                        "Change_reason",
                        "Change_Impact",
                        "Change_Description",
                        "Change_completion_date_migrated",
                        "Change_completion_date",
                        "Beneficios_para_el_negocio",
                        "Beneficio",
                        "Baseline_start_date",
                        "Baseline_end_date",
                        "Backout_plan",
                        "Anio_de_Planeacion",
                        "Authorizer_name",
                        "Atlas_project_status",
                        "Atlas_project",
                        "Atlas_goals",
                        "Assignment_Unique_ID",
                        "Assigned_profile",
                        "Asignado",
                        "Area_to_escalate",
                        "Aprobacion_RFC",
                        "Aprobador",
                        "Area",
                        "Area_de_UN",
                        "Area_name",
                        "Change_Reasons_business_terms",
                        "Change_risk",
                        "Change_risk_migrated",
                        "Change_start_date",
                        "Change_start_date_migrated",
                        "Change_type_1",
                        "Change_type_2",
                        "Change_type_migrated",
                        "Change_type_ITSM",
                        "Checklist_Completed",
                        "Checklist_Content_YAML",
                        "Confidence_1",
                        "Confidence_2",
                        "Components",
                        "Componente_del_Sistema",
                        "Complejidad",
                        "Comments",
                        "Comentario_satisfaccion",
                        "Closure_Cause_Problems",
                        "Closure_Cause_General",
                        "Closure_Cause_Changes",
                        "Closure_Cause_Service_Request",
                        "Closure_category_TyD",
                        "Closure_category_Technical_Support",
                        "Closure_category_Systems",
                        "Closure_category_Digital_Workplace",
                        "Closure_Category_Cybersecurity",
                        "Closure_category_Communications",
                        "Close_Category_Infrastructure",
                        "Checklist_Progress",
                        "Checklist_Progress_",
                        "Checklist_Template",
                        "Checklist_Text",
                        "Checklist_Text_view_only",
                        "Classification",
                        "Clasificacion_Estrategica",
                        "Criticidad",
                        "Criterios_Aceptacion",
                        "Creator",
                        "Costo_estimado_USD",
                        "Correo_Nombre_de_usuario",
                        "Conversacion",
                        "Contribucion",
                        "Customer_segments",
                        "Date_of_hire",
                        "Delivery_progress",
                        "Delivery_status",
                        "Department_1",
                        "Department_2",
                        "Department_to_scalate",
                        "Cuenta_de_correo_solicitante",
                        "Customer_resolution",
                        "Designs_ready",
                        "Design",
                        "Description",
                        "Descripcion_del_problema",
                        "Descripcion_del_evento",
                        "Descripcion_de_la_solucion",
                        "Descripcion",
                        "Describe_la_causa_raiz",
                        "Dependencia",
                        "Development",
                        "Device_serial_number",
                        "Did_you_receive_a_suspicious_email_",
                        "Did_you_receive_an_attached_file_",
                        "Documents",
                        "Duracion",
                        "E2E_Process",
                        "Economic_impact",
                        "Ecuacion_de_valor",
                        "Effective_Hours_Level_1",
                        "Effective_Hours_Level_2",
                        "Effort_1",
                        "Effort_2",
                        "Employee_location",
                        "Employee_number",
                        "Employee_number_Assigned_engineer",
                        "Employment_type",
                        "En_Espera_de_Validacion_de_Usuario",
                        "End_date",
                        "End_date_deprecated",
                        "End_of_the_month",
                        "Environment",
                        "Epic_Color",
                        "Epic_Name",
                        "Epic_Status",
                        "Equipo",
                        "Escenario",
                        "Esfuerzo",
                        "Espera_de_apoyo_de_area",
                        "Espera_de_aprobacion_de_usuario",
                        "Estimated_delivery_time",
                        "Estimation_ITSM",
                        "Fecha",
                        "Fecha_arranque_trabajo",
                        "Fecha_de_Cierre",
                        "Fecha_de_Entrega",
                        "Fecha_de_entrega_deseada",
                        "Fecha_de_liberacion",
                        "Fecha_de_pago",
                        "Fecha_terminacion_trabajo_PO",
                        "Folder_name",
                        "Flagged",
                        "Fix_versions",
                        "First_time_to_resolution_date",
                        "Fecha_terminacion_trabajo_DevTeam",
                        "Fecha_Promesa",
                        "Fecha_Inicio_Ciclo",
                        "Fecha_esperada_de_entrega",
                        "Fecha_del_ultimo_correo_que_recibio_de_este_destinatario",
                        "Frecuencia",
                        "Gerencia_Assets",
                        "Gerencia_Acc_Assets",
                        "Goal",
                        "Goals",
                        "Grupo_Estadistico_1",
                        "Herramienta",
                        "Hiring_department",
                        "Hiring_manager",
                        "Historial_de_Prioridad",
                        "Historial_de_Release_Date",
                        "Home_DEACERO_Service",
                        "Hostname_del_equipo",
                        "ID_",
                        "ID_Hoja_Padre_Confluence",
                        "ID_STI",
                        "Idea_archived",
                        "Idea_archived_by",
                        "Idea_archived_on",
                        "Idea_short_description",
                        "Images",
                        "Impact_1",
                        "Impact_2",
                        "Impact_migrated",
                        "Impact_ITSM",
                        "Impact_on_the_business_if_the_change_is_not_approved",
                        "Impact_Problem_ITSM",
                        "Impact_score",
                        "Impacto_a_negocio",
                        "Implementation_plan",
                        "Implementation_results",
                        "Improve_request_reason",
                        "INC_Categorias_de_Cierre_Abastecimientos",
                        "INC_Closure_Categories_Communications",
                        "INC_Closure_Categories_Digital_Workplace",
                        "INC_Closure_Categories_Infrastructure",
                        "INC_Closure_Categories_Technical_Support",
                        "INC_Closure_Category_Cybersecurity",
                        "Incident_Closure",
                        "Incidente_por_correo",
                        "Indicadores_MCEC",
                        "Initiative_Theme",
                        "Insights_1",
                        "Insights_2",
                        "Investigation_reason",
                        "Involved_visibility",
                        "Is_it_a_Known_Error_",
                        "Issue_color",
                        "IT_Notes",
                        "ITSM_Business_Unit",
                        "Jefe_de_Servicio",
                        "JIRA_type_of_board",
                        "Job_role",
                        "Job_title",
                        "Labels",
                        "Last_Viewed",
                        "Last_working_day",
                        "Leave_type",
                        "Level_2_Responsible",
                        "Liga_de_descarga_en_caso_de_ser_in_house",
                        "Linked_assets",
                        "Linked_issues_1",
                        "Linked_Issues_2",
                        "Listado_de_correos_que_no_se_reciben",
                        "Listado_de_correos_dominios_a_bloquear",
                        "Location",
                        "Location_1",
                        "Location_2",
                        "Locked_forms",
                        "Macroproceso_MIP_Principal_Assets",
                        "Macroproceso_MIP_Secundario_Assets",
                        "Major_incident",
                        "Major_incident_reason",
                        "Manager",
                        "Mejora_MCEC",
                        "Mejora_para_sistemas_o_procesos",
                        "Mejora_Tipo_MCEC",
                        "Mensaje_de_error",
                        "Mensaje_rechazo_cancelacion_Statuspage",
                        "Motivo",
                        "Move_date",
                        "MTTA",
                        "Modulo_Salesforce",
                        "Modulos_Salesforce_multiple",
                        "Name_of_the_equipment_to_install",
                        "New_server_name",
                        "Nivel_de_Soporte_Asset",
                        "Nombre_de_la_App",
                        "Nombre_del_sitio",
                        "Number_of_incidents",
                        "Objetivo_de_la_mejora_MCEC",
                        "OC",
                        "Open_forms",
                        "Operational_categorization",
                        "Opsgenie_Alert",
                        "Organizations",
                        "Origin_of_request",
                        "Original_estimate",
                        "parent",
                        "Pending_by",
                        "Pending_reason",
                        "Planned_end_date",
                        "Planned_start_date",
                        "Platform",
                        "Platform_Asset",
                        "Platform_ITSM",
                        "PM_Servicios_de_Llamada",
                        "Portfolio",
                        "Possible_interruption",
                        "Priority",
                        "Priority_CCB",
                        "Priority_2_Options",
                        "Priority_3_Options",
                        "Priority_4_Options",
                        "Priority_INC",
                        "Probability",
                        "Problem_impact",
                        "Proceso_End_to_End",
                        "Product_Area",
                        "Product_categorization",
                        "Program_Increment",
                        "Progress",
                        "Project",
                        "Project_budget",
                        "Project_name_1",
                        "Project_name_2",
                        "Project_overview_key",
                        "Project_overview_status",
                        "Project_start",
                        "Project_target",
                        "Rank",
                        "Reach",
                        "Reason",
                        "Rejection_cause_Incidents",
                        "Rejection_cause_Service_Request",
                        "Rejection_reason",
                        "Remaining_Estimate",
                        "Reporter_Location_ITSM",
                        "Request_Date",
                        "Request_language",
                        "Request_participants",
                        "Request_Type",
                        "Requieres_MIRO",
                        "Requirement",
                        "Resignation_date",
                        "Resolution_Date",
                        "Resolution_STI",
                        "Resolved",
                        "Responders",
                        "Resultado_Esperado_MCEC",
                        "Risk",
                        "Roadmap",
                        "ROI",
                        "Root_cause",
                        "Root_Cause_Systems",
                        "Type_of_request",
                        "Satisfaction",
                        "Script",
                        "SDR",
                        "Security_Level",
                        "Sentiment",
                        "Serial_del_dispositivo",
                        "Server_name",
                        "Server_name_to_replace",
                        "Service_Category",
                        "Service_Criticality",
                        "Service_Request_Closure",
                        "Service_to_scale",
                        "Severity",
                        "Sistema_a_escalar",
                        "Sistema_ESM",
                        "Sistema_ITSM",
                        "SLA_expiration_MTTA",
                        "SLA_expiration_Time_to_Escalate",
                        "SLA_expiration_Time_to_reasing",
                        "SLA_expiration_Time_to_resolution",
                        "Software_ID",
                        "Solicitado_por",
                        "Solicitudes_Abastecimientos",
                        "Solicitudes_abastecimientos_Assets",
                        "Solicitudes_finanzas_Assets",
                        "Source",
                        "Spec_ready",
                        "Sprint",
                        "Start_date",
                        "Status_Category",
                        "Status_Category_Changed",
                        "Status_de_ticket",
                        "Statuspage_Component_ID",
                        "Statuspage_Component_Status",
                        "Statuspage_Incident_ID",
                        "Statuspage_message_Identified",
                        "Statuspage_Message_Monitoring",
                        "Statuspage_Message_Resolved",
                        "Statuspage_name",
                        "Story_point_estimate",
                        "Story_Points",
                        "Story_Points_Real",
                        "Sub_tasks",
                        "Subcategory",
                        "Subdireccion_Assets",
                        "Subdireccion_Acc_Assets",
                        "Submitted_forms",
                        "subtask_sla",
                        "Support_Level",
                        "System",
                        "Systems",
                        "Tiempo_de_escalacion",
                        "Tiempo_de_espera",
                        "Tiempo_de_espera_CCB",
                        "Tiempo_de_espera_de_Admon_TI",
                        "Tiempo_de_espera_de_informacion",
                        "Tiempo_de_espera_de_proveedores",
                        "Tiempo_de_espera_de_sistemas",
                        "Ticket_Type",
                        "test_sistemas_list",
                        "Test_plan",
                        "Test_cases",
                        "Telephone",
                        "Teams",
                        "Target_end",
                        "Target_start",
                        "Task_progress",
                        "Team",
                        "Tiempo_de_espera_en_informacion",
                        "Tiempo_de_reasignacion",
                        "Tiempo_de_respuesta",
                        "Tiempo_de_respuesta_de_usuario",
                        "Tiempo_de_respuesta_Autorizacion_del_usuario",
                        "Tiempo_de_Revision_Comercial",
                        "Tiempo_de_Revision_TyD",
                        "Tiempo_de_validacion_de_usaurio",
                        "Tiempo_de_validacion_de_usuario",
                        "Tiempo_Espera_del_Resolved_Closed",
                        "Tiempo_Espera_Pendiente_Rechazo",
                        "Tiempo_para_escalar_al_siguiente_nivel",
                        "Time_Spent",
                        "Time_to_close_after_resolution",
                        "Time_to_first_response",
                        "Time_to_resolution_Problem",
                        "Time_To_Resolution_Total_time",
                        "Time_to_review_normal_change",
                        "Tipo",
                        "Tipo_de_Beneficio",
                        "Tipo_de_Ubicacion",
                        "Title",
                        "Toma_de_incidentes_y_solicitudes_de_Servicio_MTTA",
                        "Tool_or_project",
                        "Total_forms",
                        "Transport_order",
                        "Ubicacion_destino",
                        "Ubicacion_que_surte",
                        "Unidad_de_Negocio_que_solicita_Assets",
                        "Urgency_1",
                        "Urgency_2",
                        "Urgency_3",
                        "URL_of_the_site",
                        "USB_type_of_request",
                        "User_name",
                        "User_picker",
                        "Usuarios_Lideres",
                        "Valor_numerico_del_beneficio_USD",
                        "Value",
                        "Votes",
                        "Vulnerability",
                        "Waiting_reason",
                        "Watchers",
                        "Sum_Time_Spent",
                        "Sum_Remaining_Estimate",
                        "Sum_Progress",
                        "Sum_Original_Estimate",
                        "area_de_negocios_potencialmente_afectados",
                        "Es_rechazado_por_ser_un_cambio_",
                        "El_Proyecto_tiene_Business_Case_",
                        "Diste_clic_en_algun_link_",
                        "zServicio_OD",
                        "WTI_Version",
                        "WTI_Sync_Status",
                        "WTI_Sistema",
                        "WTI_Pagina",
                        "WTI_Producto",
                        "WTI_Modulo_deprecated",
                        "WTI_Modulo",
                        "WTI_Id_de_mejora",
                        "WTI_Id_de_funcionalidad",
                        "WTI_Id_Caso_de_Uso",
                        "WTI_Estatus",
                        "WTI_Complejidad",
                        "Workstream",
                        "Worker_type",
                        "Workaround",
                        "Work_Ratio",
                        "Work_category",
                        "Who_needs_permission_",
                        "What_was_done_in_the_incient_"]

    # Connect to Jira
    jira = connect_to_jira()

    # Set up date range for filtering
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)

    # Fetch issues
    issues = fetch_jira_issues(jira, start_date, end_date)

    # Prepare data for BigQuery
    rows_to_insert, schema = prepare_data_for_bigquery(jira, issues, custom_field_mapping, schema_json,
                                                       bigquery_columns)

    # Connect to BigQuery
    bq_client = connect_to_bigquery()

    # Upload data to BigQuery
    success = upload_to_bigquery(bq_client, rows_to_insert, schema)

    if success:
        print("Data upload completed successfully.")
    else:
        print("Data upload encountered errors. Please check the logs.")


if __name__ == "__main__":
    main()