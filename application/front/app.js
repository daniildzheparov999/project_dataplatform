const routes=[
          {path:'/home',component:home},
          {path:'/employee',component:employee},
          {path:'/department',component:department},
          {path:'/worklog',component:worklog}
      ]
      
      const router = VueRouter.createRouter({
          history: VueRouter.createWebHashHistory(),
          routes
        })
        const app = Vue.createApp({})
        app.use(router)
        app.mount('#app')