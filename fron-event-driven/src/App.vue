<template>
  <v-app>
    <v-toolbar color="blue darken-4" app fixed :clipped-left="$vuetify.breakpoint.lgAndUp" v-on:click.native="deployMenu()">
      <v-toolbar-side-icon></v-toolbar-side-icon>
      <v-toolbar-title class="white--text">Front Event Driven</v-toolbar-title>
      <v-spacer></v-spacer>
    </v-toolbar>
    <v-navigation-drawer app stateless permanent floating :mini-variant.sync="mini"
                         :clipped="$vuetify.breakpoint.lgAndUp">
      <v-list dense class="pt-0" >
        <v-list-tile v-for="item in items" :key="item.title" @click="" :href="item.link">
          <v-list-tile-action>
            <v-tooltip right>
              <v-icon slot="activator" :color="item.color">{{ item.icon }}</v-icon>
              <span>{{ item.title }}</span>
            </v-tooltip>
          </v-list-tile-action>
          <v-list-tile-content>
            <v-list-tile-title>{{ item.title }}</v-list-tile-title>
          </v-list-tile-content>
        </v-list-tile>
      </v-list>
    </v-navigation-drawer>
    <v-content>
      <router-view></router-view>
    </v-content>
    <!-- <v-footer app></v-footer> -->

  </v-app>
</template>

<script>
  export default {
    data() {
      return {
        userDefine: '',
        mini: true,
        app: this,
        items: [
          {title: 'Home', icon: 'dashboard', color: 'blue darken-2', link: '/home'},
          {title: 'Action Configuration', icon: 'settings', color: 'green darken-2', link: '/action'},
          {title: 'Kafka Live', icon: 'call_split', color: 'blue-grey darken-2', link: '/live'}
        ]
      }
    },
    mounted() {

    },
    methods: {
      deployMenu() {
        this.mini = !this.mini;
      }
    }
  }
</script>
