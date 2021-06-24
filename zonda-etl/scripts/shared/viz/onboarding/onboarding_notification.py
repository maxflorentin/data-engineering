import os
import smtplib
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from itertools import groupby
from pathlib import Path
import psycopg2
import psycopg2.extras
from airflow.hooks.base_hook import BaseHook


def run_script():
    # conecta con la base de datos de tableau
    airflow_conn = BaseHook.get_connection("tableau_db_ro")
    conn = psycopg2.connect(user=airflow_conn.login,
                            password=airflow_conn.password,
                            host=airflow_conn.host,
                            port=airflow_conn.port,
                            database=airflow_conn.schema)

    # define cursor para la conexión con PostgreSQL
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # query
    cur.execute("""select uv.email,
       u.friendly_name,
       luid,
       licencia,
       site_id,
       max(created_at::timestamp::date) as creado
        from
        (SELECT user_info.id,
       user_info.name,
       user_info.login_at,
       user_info.created_at,
       user_info.friendly_name,
       tier_mapping.licensing_role_id,
       tier_mapping.licensing_role_name::character varying(255) AS licensing_role_name,
       user_info.domain_id,
       user_info.system_user_id,
       user_info.domain_name,
       user_info.domain_short_name,
       user_info.site_id,
        user_info.licencia,
        user_info.luid
        FROM (SELECT users.id,
             system_users.name,
             site_roles.display_name as licencia,
             users.login_at,
             users.created_at,
             system_users.friendly_name,
             first_value(site_roles.id)
             OVER (PARTITION BY users.system_user_id ORDER BY site_roles.licensing_rank DESC) AS max_site_role_id,
             system_users.domain_id,
             users.system_user_id,
             domains.name                                                                     AS domain_name,
             domains.short_name                                                               AS domain_short_name,
             users.site_id,
            users.luid
        FROM system_users
               JOIN users ON users.system_user_id = system_users.id
               JOIN domains ON system_users.domain_id = domains.id
               JOIN site_roles ON users.site_role_id = site_roles.id) user_info
         JOIN get_licensing_role_for_site_role() tier_mapping(site_role_id, licensing_role_id, licensing_role_name)
              ON tier_mapping.site_role_id = user_info.max_site_role_id
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14) u
        inner join users_view uv on uv.name = u.name
        where
        email is not null
        and email is not null
    and licencia not like '%Unlicensed'
     and u.name not in ('A018385','A305885','A010307','A309192','A304886','A306786','A307684','A307792','A309195','A309199','A307681',
            'A307626','A223971','A307775','A307405','A309203','A309414','A307834','A308439','A308584','A308343','A308156','A307924','A307624','A307212','A299794')
    and created_at::timestamp between NOW() - INTERVAL '15 minutes' and now()
    and (site_id = 2 or site_id= 1)
    group by 1,2,3,4,5
    order by creado desc;
    """)

    # guarda los resultados del SELECT
    rows = cur.fetchall()

    # envía información a la lista.
    groups = groupby(rows, key=lambda e: e.get("email") + "|" + e.get("friendly_name").split(",")[-1] + "|" + e.get("licencia"))
    results = []

    for key, extractos in groups:
        emails, friendly_name, licencia = key.split("|")
        json = dict()
        json["email"] = emails
        json["friendly_name"] = friendly_name
        json["licencia"] = licencia

        results.append(json)
        print(json)

    # comienza el envio de emails
    smtpserver = smtplib.SMTP("relay.ar.bsch", 25)
    smtpserver.set_debuglevel(False)

    for r in results:
        body = """\
                    <html>
                    <body>
                    <div align="center">
                    <img src="cid:imagen.png"/>
                    </div>
                        <p>¡Hola <b>{}</b>!
                            <br>
                            <br>Ya podés ingresar a nuestra plataforma a través del siguiente link <a href="https://tableau.santander.com.ar"> https://tableau.santander.com.ar</a> .
                            <br>
                            <br>
                            Seguí los lineamientos que hemos creado para vos en la <a href=https://tableau.santander.com.ar/t/LabData/views/PlantillaTableau/Portada?iframeSizedToWindow=true&:embed=y&:showAppBanner=false&:display_count=no&:showVizHome=no&:origin=viz_share_link#1> Guía de mejores prácticas de Tableau </a>.
                            <br>
                            <br>
                            Por consultas, nos podés escribir a <a href="mailto:DATA-VIZ@santanderrio.com.ar"> DATA-VIZ@santanderrio.com.ar</a>
                            <br>
                            <br> <i> Atte. Equipo Data-Viz.
                        </p>
                    </body>
                    </html>
                    """.format(r["friendly_name"])
        msg = MIMEMultipart()
        msg_content = MIMEText(body, 'html')
        msg.attach(msg_content)
        fp = open(str(Path(os.path.dirname(os.path.realpath(__file__))).parent) + '/imagen.png', 'rb').read()
        msgImage = MIMEImage(fp, 'png')
        msgImage.add_header('Content-ID', '<image1>')
        msgImage.add_header('Content-Disposition', 'inline', filename="imagen.png")
        msg.attach(msgImage)

        # dataViz='Data-viz@santanderrio.com.ar'
        # cc=dataViz
        msg['To'] = r["email"]
        # msg['To'] = 'ggonzalezwallis@santandertecnologia.com.ar'
        msg['BCc'] = 'ggonzalezwallis@santandertecnologia.com.ar,cristiramirez@santandertecnologia.com.ar,itroisi@santandertecnologia.com.ar,ajimenezbacca@santandertecnologia.com.ar,dtridico@santandertecnologia.com.ar,jmedinapena@santandertecnologia.com.ar,pabnunez@santandertecnologia.com.ar,sbruzatoriharper@santandertecnologia.com.ar'
        # msg['BCc'] = 'maurorodriguez@santandertecnologia.com.ar'
        msg['From'] = 'TABLEAU@santanderrio.com.ar'
        msg['Subject'] = '[IMPORTANTE] - Tableau - ¡Bienvenido!'
        rcpt = msg['BCc'].split(",") + [msg['To']]

        try:
            smtpserver.sendmail(msg.get('From'),
                                rcpt,
                                msg.as_string())
        except Exception as ex:
            print("Error sending email to {} [{}]".format(msg['To'], str(ex)))
            continue

    if not results:
        print("No user registrations were detected")


if __name__ == '__main__':
    run_script()
