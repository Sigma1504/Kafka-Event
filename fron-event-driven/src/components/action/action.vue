<template>
 <v-container fluid grid-list-md>
      <v-layout row xs4 sm4 md4>
        <v-flex xs4 sm4 md4 >
          <v-select label="Stream Name" v-model="streamItem.streamName" v-bind:items="listStream" required/>
          <v-text-field label="Instance Name" v-model="streamItem.instanceName" ></v-text-field>
          <v-select label="Type Action" v-model="streamItem.typeAction" v-bind:items="typeAction" required/>
        </v-flex>
      </v-layout>
    <v-btn color="primary" v-on:click.native="launchAction()">Launch Action</v-btn>
    <v-layout row>
      <v-flex xs12 sm12 md12 >
        <v-alert v-model="viewError" xs12 sm12 md12  color="error" icon="warning" value="true" dismissible>
             {{ msgError }}
        </v-alert>
         <v-alert v-model="viewAction" xs12 sm12 md12  color="success" icon="warning" value="true" dismissible>
               {{ msgAction }}
          </v-alert>
       </v-flex>
    </v-layout>
 </v-container>
</template>


<script>
  export default{
    data () {
         return {
           msgAction: '',
           viewAction: false,
           msgError: '',
           viewError: false,
           headers: [
             { text: 'Stream name',align: 'center', width: '40%'},
             { text: 'Instance', align: 'center',value: '',width: '20%' },
             { text: 'Event Status',align: 'center', value: '', width: '10%' },
             { text: 'Status',align: 'center', value: '', width: '10%' },
             { text: 'Date Generation',align: 'center', value: '', width: '20%' },
             { text: 'Details',align: 'center', value: '', width: '20%' },
           ],
           streamItem: {streamName: "",typeAction: "",instanceName: "",eventStatus: "",status: "",dateGeneration: "", mapLabels: []},
           headersLabels: [
              { text: 'Key',align: 'center', width: '50%'},
              { text: 'Value', align: 'center',width: '50%' }
           ],
           typeAction: ["START","START_ALL","STOP","FORCE_STOP","STOP_ALL","DELETE","DELETE_ALL"],
           listStream: []
         }
    },
    mounted() {
         this.$http.get('/manage/findAll').then(response => {
            var fullListStream=response.data;
            this.listStream = fullListStream.map(item => item.streamName);
         }, response => {
           this.viewError=true;
           this.msgError = "Error during call service";
         });
    },
    methods: {
        launchAction(){
          this.$http.post('/manage/action',{streamName: this.streamItem.streamName, typeAction: this.streamItem.typeAction, instanceName: this.streamItem.instanceName}).then(response => {
              this.msgAction= "Ok";
              this.viewAction=true;
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
        }
    }
  }
</script>
