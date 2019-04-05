<template>
 <v-container fluid grid-list-md>
    <v-layout row wrap>
      <v-card xs12 sm12 md12>
        <v-card-title>
          <v-text-field label="Instance Name" v-model="instanceNameSearch" max-width="200px"></v-text-field>
          <v-btn color="primary" v-on:click.native="searchByInstanceName">search </v-btn>
          <v-tooltip right>
            <v-btn :disabled="listStream.length > 0 ? false : true" slot="activator" flat v-on:click.native="refreshAction" icon color="blue lighten-2">
              <v-icon>refresh</v-icon>
            </v-btn>
            <span>Refresh the list</span>
          </v-tooltip>
          <v-spacer></v-spacer>
        </v-card-title>
        <v-data-table v-bind:headers="headers" :items="listStream" >
          <template slot="items" slot-scope="props">
              <td class="text-xs-center">{{props.item.streamName}}</td>
              <td class="text-xs-center">{{props.item.instanceName}}</td>
              <td class="text-xs-center">{{props.item.eventStatus}}</td>
              <td class="text-xs-center">{{props.item.status}}</td>
              <td class="text-xs-center">{{new Date(props.item.dateGeneration).toUTCString()}}</td>
              <td class="text-xs-center">
                <v-btn slot="activator" icon v-on:click.native="showDetails(props.item.streamName)" ><v-icon color="green">play_circle_filled</v-icon></v-btn>
              </td>
          </template>
        </v-data-table>
      </v-card>
    </v-layout>
    <v-layout row wrap>
      <v-flex xs12 sm12 md12 >
        <v-alert v-model="viewError" xs12 sm12 md12  color="error" icon="warning" value="true" dismissible>
             {{ msgError }}
        </v-alert>
       </v-flex>
    </v-layout>

   <v-dialog v-model="dialogDetails" max-width="560px">
      <v-card>
        <v-card-title primary-title><h3>Details</h3></v-card-title>
        <v-card-text>
            <v-layout row wrap>
              <v-text-field label="Stream Name" v-model="streamItem.streamName" readonly></v-text-field>
            </v-layout>
            <v-layout row wrap>
              <v-text-field label="Instance Name" v-model="streamItem.instanceName" readonly></v-text-field>
            </v-layout>
            <v-layout row wrap>
              <v-text-field label="Status Event" v-model="streamItem.eventStatus" readonly></v-text-field>
              <v-text-field label="Status Stream" v-model="streamItem.status" readonly></v-text-field>
              <v-text-field label="Date Actualisation" v-model="new Date(streamItem.dateGeneration).toUTCString()" readonly></v-text-field>
            </v-layout>
            <v-layout row wrap>
                <v-data-table v-bind:headers="headersLabels" :items="streamItem.mapLabels" hide-actions>
                    <template slot="itemsLabel" slot-scope="propsLabel">
                      <td class="text-xs-center">{{propsLabel.itemsLabel.key}}</td>
                      <td class="text-xs-center">{{propsLabel.itemsLabel.value}}</td>
                    </template>
                </v-data-table>
            </v-layout>

        </v-card-text>
        <v-card-actions>
          <v-btn color="error" @click.stop="dialogDetails=false">Close<v-icon right>close</v-icon></v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>

 </v-container>
</template>


<script>
  export default{
    data () {
         return {
           listStream: [],
           msgError: '',
           viewError: false,
           headers: [
             { text: 'Stream name',align: 'center', width: '40%'},
             { text: 'Instance', align: 'center',value: '',width: '20%' },
             { text: 'Event Status',align: 'center', value: '', width: '10%' },
             { text: 'Status',align: 'center', value: '', width: '10%' },
             { text: 'Date Actualisation',align: 'center', value: '', width: '20%' },
             { text: 'Details',align: 'center', value: '', width: '20%' },
           ],
           dialogDetails: false,
           streamItem: {streamName: "",typeAction: "",instanceName: "",eventStatus: "",status: "",dateGeneration: "", mapLabels: []},
           headersLabels: [
              { text: 'Key',align: 'center', width: '50%'},
              { text: 'Value', align: 'center',width: '50%' }
           ],
           instanceNameSearch: ''
         }
    },
    mounted() {
        this.refreshAction();
    },
    methods: {
        refreshAction(){
          this.$http.get('/manage/findAll').then(response => {
              this.listStream=response.data;
          }, response => {
             this.viewError=true;
             this.msgError = "Error during call service";
          });
        },
        showDetails(streamName){
          this.$http.get('/manage/findByStreamName',{params : {streamName: streamName}}).then(response => {
              this.streamItem=response.data;
              this.dialogDetails=true;
              console.log('streamName '+streamName);
           }, response => {
             this.viewError=true;
             this.msgError = "Error during call service";
           });
        },
        searchByInstanceName(){
           this.$http.get('/manage/findByInstanceName',{params : {instanceName: this.instanceNameSearch}}).then(response => {
              this.listStream=response.data;
           }, response => {
             this.viewError=true;
             this.msgError = "Error during call service";
           });
        }
    }
  }
</script>
