const fs = require('fs')
const path = require('path')
const say = require('say')

const samples = [
  'Quick market update. Tech stocks moved higher in early trading as investors reacted to better than expected cloud revenue guidance. The benchmark index gained one point two percent, while volatility cooled for a second straight session. Analysts now expect momentum to remain positive if inflation data comes in stable this week.',
  'Midday note for traders. Banking shares recovered after management commentary suggested strong deposit growth and improving credit quality. Small cap names outperformed large cap peers, and market breadth stayed healthy across sectors. Risk sentiment improved as bond yields eased and option flow turned more balanced.',
  'Evening wrap. Energy and semiconductor names led gains after fresh policy announcements and stronger demand indicators. Market participants rotated into quality growth while defensive sectors lagged. Attention now shifts to tomorrow earnings reports, where guidance clarity may decide the next directional move.'
]

const pickRandomText = () => samples[Math.floor(Math.random() * samples.length)]

const outputDir = path.join(__dirname, '..', 'tmp')
const outputPath = path.join(outputDir, 'say-demo.wav')

fs.mkdirSync(outputDir, { recursive: true })

const text = pickRandomText()

say.export(text, undefined, 1.0, outputPath, err => {
  if (err) {
    console.error('Failed to generate demo audio:', err.message)
    process.exit(1)
  }

  console.log('Demo audio generated successfully.')
  console.log('Output:', outputPath)
  console.log('Sample text:', text)
})
