import Vue from 'vue'
import VueRouter from 'vue-router'

Vue.use(VueRouter)


let router = new VueRouter({
    mode: 'history',
    routes: [
      {
        path: '/home',
        component: require('../../components/process/home.vue'),
        name: 'main'
      },
      {
        path: '/action',
        component: require('../../components/action/action.vue'),
        name: 'action'
      },
      {
        path: '/live',
        component: require('../../components/live/live.vue'),
        name: 'live'
      },
      {
        path: '*',
        redirect: '/home'
      }
    ]
  }
)

export default router
