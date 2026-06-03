import { createApp } from 'vue'
import { createPinia } from 'pinia'

import TdxApp from './TdxApp.vue'
import tdxRouter from './router/tdx'
import './styles/main.scss'

const app = createApp(TdxApp)

app.use(createPinia())
app.use(tdxRouter)

app.mount('#app')
