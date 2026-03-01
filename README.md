El objetivo es responder a la eterna pregunta: "¿Qué tiempo va a hacer hoy?", pero desde una perspectiva funcional. No nos importa solo si llueve, sino si es un buen día.

Los datos se extraen en tiempo real de la API de MeteoGalicia, garantizando precisión y localización específica en la geografía gallega.

1. Dashboard para actividades en tierra:
  Se centra en la comodidad del usuario fuera del entorno urbano.

  - ¿Qué me pongo?: un sistema de recomendaciones inteligente que, basándose en la temperatura, el viento, la lluvia y la niebla, te sugiere desde capas ligeras hasta equipamiento impermeable.
  - Índice Día Sludable: una métrica personalizada donde el usuario puede definir qué considera un "buen día" y ver una puntuación del 0 al 100.
  - Pronósticos climáticos: mediante una ventana temporal modulable por el usuario se visualiza por horas de forma clara la medida que se escoja.
   

2. Dashboard para actividades en mar:
  Responde a cusetiones prácticas para realizar deportes acuáticos tanto de vela como de deslizamiento acuático por inercia.

  - ¿Me voy a pelar de frío al sacarme el neopreno?: una métrica que calcula la sensación de frío al salir del agua. Es ideal para decidir si te cambias tranquilamente o corres al coche.
  - Visibilidad en el mar: análisis de la capa nubosa para predecir la visibilidad en el spot.
  - Datos concretos sobre datos relevante marítimos.

Tecnologías Utilizadas:
Fuente de datos: MeteoGalicia API
Visualización: Grafana
Base de Datos: InfluxDB

Nota Curiosa: Este proyecto nació de la necesidad de saber si valía la pena cargar la tabla de surf en el coche o si era mejor quedarse en casa tomando un café viendo cómo llueve (que en Galicia también es un gran plan, sobre todo si es de pota).
