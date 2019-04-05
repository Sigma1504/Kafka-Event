import Vue from 'vue'
import App from './App'
import Vuetify from 'vuetify'
import Vuex from 'vuex'
import Vuechartjs from 'vue-chartjs';
import VueResource from 'vue-resource';
import TreeView from 'vue-json-tree-view';
import router from './service/router/Router';
import 'vuetify/dist/vuetify.min.css';

Vue.use(TreeView);
Vue.use(Vuex);
Vue.use(Vuetify);
Vue.use(VueResource);
Vue.use(Vuechartjs);

router.beforeEach((to, from, next) => {
document.title = "Front Event Driven";
 next();
})

window.bus = new Vue();

new Vue({
  el: '#app',
  router,
  render: h => h(App)
})
