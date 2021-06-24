import bootstrap_rosemeta
from rosemeta.models.models_data import Attribute

apellidos = ['apellido', 'pepriape', 'pesegape', '_ape', 'apell']
mails = ['mail', 'correo_electronico']
cuits = ['cuit', 'cuil']
domicilios = ['domicilio', 'domic', 'direc_fisica', 'domant', 'secdom', 'titdom', 'direcc_visita', 'direccion', 'dom_',
              '_dom', 'calle']
documentos = ['nro_doc', 'numdoc', 'num_doc', 'dni', 'numero_documento']
nombres = ['nom_benef', 'nomper', 'nom_cli', 'nomemp', 'nomcal', 'nombre_cliente', 'nom_firmante', 'nombre_firmante',
           'nom_per', 'nomnot', 'c_nombre', 'per_nombre', 'nombre_titular', 'nombre_emitido_a', 'nombrebenef',
           'nombreemitidoa']
tarjetas = ['nro_tarjeta', 'nro_tar', 'numctare', 'num_tarj', 'numero_tarjeta']
razon_social = ['razon_social', 'razonsocial']
telefonos = ['pecartel', 'telefono', 'numtel', 'sectel', 'pretel', 'celular', '_tel_', 'tel_', '_tel']

sensitive_fields = [apellidos, mails, cuits, domicilios, documentos, nombres, tarjetas, razon_social, telefonos]

for fields in sensitive_fields:

    regex = '(' + '|'.join(fields) + ')'

    for a in Attribute.objects.filter(name__regex='{}'.format(regex)):
        a.sensitive = True
        a.save(update_fields=['sensitive'])

for a in Attribute.objects.filter(name='nombre'):
    a.sensitive = True
    a.save(update_fields=['sensitive'])
