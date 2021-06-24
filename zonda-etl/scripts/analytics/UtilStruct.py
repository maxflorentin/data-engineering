# coding=utf-8

STOP_WORDS = [u"0",u"1",u"2",u"3",u"4",u"5",u"6",u"7",u"8",u"9",u"_", u'a',u"actualmente",u"acuerdo",u"adelante",u"ademas",u"además",u"adrede",u"afirmó",u"agregó",u"ahi",u"ahora",u"ahí",
"al",u"algo",u"alguna",u"algunas",u"alguno",u"algunos",u"algún",u"alli",u"allí",u"alrededor",u"ambos",u"ampleamos",u"antano",u"antaño",u"ante",u"anterior",u"antes",u"apenas",
"aproximadamente",u"aquel",u"aquella",u"aquellas",u"aquello",u"aquellos",u"aqui",u"aquél",u"aquélla",u"aquéllas",u"aquéllos",u"aquí",u"arriba",u"arribaabajo",u"aseguró",u"asi",u"así",
"atras",u"aun",u"aunque",u"ayer",u"añadió",u"aún",u"b",u"bajo",u"bastante",u"bien",u"breve",u"buen",u"buena",u"buenas",u"bueno",u"buenos",u"c",u"cada",u"casi",u"cerca",u"cierta",u"ciertas",
"cierto",u"ciertos",u"cinco",u"claro",u"comentó",u"como",u"con",u"conmigo",u"conocer",u"conseguimos",u"conseguir",u"considera",u"consideró",u"consigo",u"consigue",u"consiguen",u"consigues",
"contigo",u"contra",u"cosas",u"creo",u"cual",u"cuales",u"cualquier",u"cuando",u"cuanta",u"cuantas",u"cuanto",u"cuantos",u"cuatro",u"cuenta",u"cuál",u"cuáles",u"cuándo",u"cuánta",u"cuántas",
"cuánto",u"cuántos",u"cómo",u"d",u"da",u"dado",u"dan",u"dar",u"de",u"debajo",u"debe",u"deben",u"debido",u"decir",u"dejó",u"del",u"delante",u"demasiado",u"demás",u"dentro",u"deprisa",u"desde",
"despacio",u"despues",u"después",u"detras",u"detrás",u"dia",u"dias",u"dice",u"dicen",u"dicho",u"dieron",u"diferente",u"diferentes",u"dijeron",u"dijo",u"dio",u"donde",u"dos",u"durante",u"día",
"días",u"dónde",u"e",u"ejemplo",u"el",u"ella",u"ellas",u"ello",u"ellos",u"embargo",u"empleais",u"emplean",u"emplear",u"empleas",u"empleo",u"en",u"encima",u"encuentra",u"enfrente",u"enseguida",
"entonces",u"entre",u"era",u"erais",u"eramos",u"eran",u"eras",u"eres",u"es",u"esa",u"esas",u"ese",u"eso",u"esos",u"esta",u"estaba",u"estabais",u"estaban",u"estabas",u"estad",u"estada",u"estadas",
"estado",u"estados",u"estais",u"estamos",u"estan",u"estando",u"estar",u"estaremos",u"estará",u"estarán",u"estarás",u"estaré",u"estaréis",u"estaría",u"estaríais",u"estaríamos",u"estarían",
"estarías",u"estas",u"este",u"estemos",u"esto",u"estos",u"estoy",u"estuve",u"estuviera",u"estuvierais",u"estuvieran",u"estuvieras",u"estuvieron",u"estuviese",u"estuvieseis",u"estuviesen",
"estuvieses",u"estuvimos",u"estuviste",u"estuvisteis",u"estuviéramos",u"estuviésemos",u"estuvo",u"está",u"estábamos",u"estáis",u"están",u"estás",u"esté",u"estéis",u"estén",u"estés",u"ex",
"excepto",u"existe",u"existen",u"explicó",u"expresó",u"f",u"fin",u"final",u"fue",u"fuera",u"fuerais",u"fueran",u"fueras",u"fueron",u"fuese",u"fueseis",u"fuesen",u"fueses",u"fui",u"fuimos",
"fuiste",u"fuisteis",u"fuéramos",u"fuésemos",u"g",u"general",u"gran",u"grandes",u"gueno",u"h",u"ha",u"haber",u"habia",u"habida",u"habidas",u"habido",u"habidos",u"habiendo",u"habla",u"hablan",
"habremos",u"habrá",u"habrán",u"habrás",u"habré",u"habréis",u"habría",u"habríais",u"habríamos",u"habrían",u"habrías",u"habéis",u"había",u"habíais",u"habíamos",u"habían",u"habías",u"hace",
"haceis",u"hacemos",u"hacen",u"hacer",u"hacerlo",u"haces",u"hacia",u"haciendo",u"hago",u"han",u"has",u"hasta",u"hay",u"haya",u"hayamos",u"hayan",u"hayas",u"hayáis",u"he",u"hecho",u"hemos",
"hicieron",u"hizo",u"horas",u"hoy",u"hube",u"hubiera",u"hubierais",u"hubieran",u"hubieras",u"hubieron",u"hubiese",u"hubieseis",u"hubiesen",u"hubieses",u"hubimos",u"hubiste",u"hubisteis",
"hubiéramos",u"hubiésemos",u"hubo",u"i",u"igual",u"incluso",u"indicó",u"informo",u"informó",u"intenta",u"intentais",u"intentamos",u"intentan",u"intentar",u"intentas",u"intento",u"ir",u"j",
"junto",u"k",u"l",u"la",u"lado",u"largo",u"las",u"le",u"lejos",u"les",u"llegó",u"lleva",u"llevar",u"lo",u"los",u"luego",u"lugar",u"m",u"mal",u"manera",u"manifestó",u"mas",u"mayor",u"me",u"mediante",
"medio",u"mejor",u"mencionó",u"menos",u"menudo",u"mi",u"mia",u"mias",u"mientras",u"mio",u"mios",u"mis",u"misma",u"mismas",u"mismo",u"mismos",u"modo",u"momento",u"mucha",u"muchas",u"mucho",
"muchos",u"muy",u"más",u"mí",u"mía",u"mías",u"mío",u"míos",u"n",u"nada",u"nadie",u"ni",u"ninguna",u"ningunas",u"ninguno",u"ningunos",u"ningún",u"no",u"nos",u"nosotras",u"nosotros",u"nuestra",
"nuestras",u"nuestro",u"nuestros",u"nueva",u"nuevas",u"nuevo",u"nuevos",u"nunca",u"o",u"ocho",u"os",u"otra",u"otras",u"otro",u"otros",u"p",u"pais",u"para",u"parece",u"parte",u"partir",u"pasada",
"pasado",u"paìs",u"peor",u"pero",u"pesar",u"poca",u"pocas",u"poco",u"pocos",u"podeis",u"podemos",u"poder",u"podria",u"podriais",u"podriamos",u"podrian",u"podrias",u"podrá",u"podrán",u"podría",
"podrían",u"poner",u"por",u"por qué",u"porque",u"posible",u"primer",u"primera",u"primero",u"primeros",u"principalmente",u"pronto",u"propia",u"propias",u"propio",u"propios",u"proximo",
"próximo",u"próximos",u"pudo",u"pueda",u"puede",u"pueden",u"puedo",u"pues",u"q",u"qeu",u"que",u"quedó",u"queremos",u"quien",u"quienes",u"quiere",u"quiza",u"quizas",u"quizá",u"quizás",u"quién",
"quiénes",u"qué",u"r",u"raras",u"realizado",u"realizar",u"realizó",u"repente",u"respecto",u"s",u"sabe",u"sabeis",u"sabemos",u"saben",u"saber",u"sabes",u"sal",u"salvo",u"se",u"sea",u"seamos",
"sean",u"seas",u"segun",u"segunda",u"segundo",u"según",u"seis",u"ser",u"sera",u"seremos",u"será",u"serán",u"serás",u"seré",u"seréis",u"sería",u"seríais",u"seríamos",u"serían",u"serías",u"seáis",
"señaló",u"si",u"sido",u"siempre",u"siendo",u"siete",u"sigue",u"siguiente",u"sin",u"sino",u"sobre",u"sois",u"sola",u"solamente",u"solas",u"solo",u"solos",u"somos",u"son",u"soy",u"soyos",u"su",
"supuesto",u"sus",u"suya",u"suyas",u"suyo",u"suyos",u"sé",u"sí",u"sólo",u"t",u"tal",u"tambien",u"también",u"tampoco",u"tan",u"tanto",u"tarde",u"te",u"temprano",u"tendremos",u"tendrá",u"tendrán",
"tendrás",u"tendré",u"tendréis",u"tendría",u"tendríais",u"tendríamos",u"tendrían",u"tendrías",u"tened",u"teneis",u"tenemos",u"tener",u"tenga",u"tengamos",u"tengan",u"tengas",u"tengo",
"tengáis",u"tenida",u"tenidas",u"tenido",u"tenidos",u"teniendo",u"tenéis",u"tenía",u"teníais",u"teníamos",u"tenían",u"tenías",u"tercera",u"ti",u"tiempo",u"tiene",u"tienen",u"tienes",u"toda",
"todas",u"todavia",u"todavía",u"todo",u"todos",u"total",u"trabaja",u"trabajais",u"trabajamos",u"trabajan",u"trabajar",u"trabajas",u"trabajo",u"tras",u"trata",u"través",u"tres",u"tu",u"tus",
"tuve",u"tuviera",u"tuvierais",u"tuvieran",u"tuvieras",u"tuvieron",u"tuviese",u"tuvieseis",u"tuviesen",u"tuvieses",u"tuvimos",u"tuviste",u"tuvisteis",u"tuviéramos",u"tuviésemos",u"tuvo",
"tuya",u"tuyas",u"tuyo",u"tuyos",u"tú",u"u",u"ultimo",u"un",u"una",u"unas",u"uno",u"unos",u"usa",u"usais",u"usamos",u"usan",u"usar",u"usas",u"uso",u"usted",u"ustedes",u"v",u"va",u"vais",u"valor",
"vamos",u"van",u"varias",u"varios",u"vaya",u"veces",u"ver",u"verdad",u"verdadera",u"verdadero",u"vez",u"vosotras",u"vosotros",u"voy",u"vuestra",u"vuestras",u"vuestro",u"vuestros",u"w",u"x",u"y"
,u"ya",u"yo",u"z",u"él",u"éramos",u"ésa",u"ésas",u"ése",u"ésos",u"ésta",u"éstas",u"éste",u"éstos",u"última",u"últimas",u"último",u"últimos"]

STOP_WORDS_AD_HOC = [ 'hola', u'quiero', u'necesito', u'queria', u'quisiera', u'gracias', u'tardes', u'hice', u'mes', u'numero', u'aparece', u'realice', u'noches', u'ok', u'opcion', 
'tenia', u'deja' , u'sale', u'julio', u'único', u'xq', u'pide', u'paso', u'darme', u'algun', u'aca', u'gustaria', u'mil', u'recien', u'saver', u'necesitaria', u'podes', u'podes', u'di', 
'porq', u'br', u'seria', u'quise', u'realize', u'volver', u'puse', u'agosto', u'tema', u'queda', u'seguir', u'pasa', u'llegado', u'intente', u'sos', u'ago', u'on', u'mo']


dicc_tarjeta = [u'tarjueta', u'taejeta', u'tarjerta', u'tarjete', u'tarjetua', u'tarjetaa', u'tarjeya', u'tarjetad', u'tarjetq',
                'tarejeta', u'tarjata', u'trjeta', u'tareta', u'tarjetas', u'tarheta', u'tarjta', u'tarrjeta', u'tarjega', u'tajeta',
                'tarjenta', u'tarjwta', u'tarjetsa', u'tarjet', u'tsrjeta', u'tarbeta', u'tarjets', u'tarjrta', u'rarjeta', u'tarneta',
                'terjeta', u'tatjeta', u'trarjeta', u'tarketa', u'tarjera', u'tarjeeta', u'tqrjeta', u'tarhjeta', u'tarjjeta',
                'tbarjeta', u'tarjefa', u'yarjeta', u'targeta', u'trajeta', u'tagetas']

dicc_prestamo = [u'presramo', u'prastamo', u'prestamio', u'preatamo', u'prestomo', u'prestamp', u'pfestamo', u'peestamo', u'prrstamo',
                 'preastamo', u'prestano', u'prestaml', u'predtamo', u'orestamo', u'prestado', u'prestmo', u'prestamos', u'préstamo',
                 'prestao', u'prestsmo']

dicc_debito = [u'debibo', u'debitr', u'dejito', u'debitp', u'bebito', u'denito', u'debit', u'debio', u'devito', u'debeito', u'debite',
               'deboto', u'debiti', u'debjto', u'debiyo', u'debiro', u'débito', u'debiso', u'ddbito', u'debitl', u'dibito', u'dwbito',
               'debita', u'debitó', u'debto', u'debitos']

dicc_credito = [u'cradito', u'credio', u'creditos', u'crediti', u'cresito', u'creddito', u'crdito', u'credrito', u'crediyo', u'creedito',
                'crédito', u'crèdito', u'creditoo', u'creeito', u'cridito', u'crwdito', u'cedito', u'crefito', u'credit', u'creditl',
                'ceedito', u'credicto', u'credoto', u'acredito', u'cresdito', u'creditp', u'crediro', u'creito', u'ctedito', u'credigo',
                'credifo', u'xredito']

dicc_transferencia = [u'tfansferencia', u'transeferencia', u'transferncia', u'transferencid', u'teansferencia', u'tramsferencia',
                      'transferecia', u'transferemcia', u'trnsferencia', u'trsnsferencia', u'transferencka', u'transferenciaa',
                      'transferencias', u'transferencis', u'trabsferencia', u'yransferencia', u'transferebcia', u'tranferencia',
                      'trasferencia', u'transferengia']

dicc_consulta = [u'consults', u'consultar', u'consulto', u'cinsulta', u'consultas', u'consukta', u'consulte', u'consulté', u'consjlta',
                 'colsulta', u'consult', u'consultat', u'consuleta', u'consula', u'connsulta', u'clnsulta', u'consilta', u'cunsulta',
                 'consultá', u'comsulta']

dicc_numero = [u'numert', u'numro', u'nunero', u'nimero', u'numeros', u'numeri', u'numeeo', u'nuero', u'número', u'nuemero', u'umero',
               'numeto']

dicc_extraccion = [u'exteaccion', u'exgraccion', u'extrccion', u'ectraccion', u'estraccion', u'extracción', u'extracion',
                   'wxtraccion', u'extracvion']

dicc_problema = [u'preoblema', u'peoblema', u'problemas', u'problena', u'priblema', u'problrma', u'pronlema', u'prblema']

dicc_renovacion = [u'renovación', u'renobacion', u'renovacio', u'renivacion', u'renovasion']




def corregir(palabra):
    if palabra in dicc_tarjeta:
        return 'tarjeta'
    elif palabra in dicc_prestamo:
        return 'prestamo'
    elif palabra in dicc_debito:
        return 'debito'
    elif palabra in dicc_credito:
        return 'credito'
    elif palabra in dicc_transferencia:
        return 'transferencia'
    elif palabra in dicc_consulta:
        return 'consulta'
    elif palabra in dicc_numero:
        return 'numero'
    elif palabra in dicc_extraccion:
        return 'extraccion'
    elif palabra in dicc_problema:
        return 'problema'
    elif palabra in dicc_renovacion:
        return 'renovacion'
    else:
        return palabra
