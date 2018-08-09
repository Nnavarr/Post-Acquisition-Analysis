

library(shiny)
library(shinydashboard)
library(shinythemes)
library(plotly)
library(dplyr)
library(ggplot2)
library(markdown)


# Data included
  includeMarkdown("Quarterly Acquisitions Report.rmd")



  


# Dashboard Architecture ----
  header <- dashboardHeader( )
  
  
  
  
  
  # Side Bar ----
  sidebar <- dashboardSidebar(
    sidebarMenu(
      
      # Create Menu
        menuItem(text = "Dashboard",
                 tabName = "dashboard"
                 )
    )
  )
  

  
  # Body ----
  body <- dashboardBody(
    fluidRow(
      
      # Row 1
        box(
          width = 12,
          title = "Income Statement",
          
          # Start of Graph 
          mainPanel(
            plotlyOutput("lineitem"),
            verbatimTextOutput('Line Item')
          )
            
          
          )
          
          
        )
      
    )
  


# Define UI for application (Dashboard) ----
ui <- dashboardPage(header = header,
                    sidebar = sidebar,
                    body = body)
   
 


# Define server logic required to draw a histogram
server <- function(input, output) {

   output$lineitem <- renderPlotly({
  
     
      plot_ly(Data.Combined, x = ~Date, y = ~Value, mode = 'lines')
     
   })
} 
  

# Run the application 
shinyApp(ui = ui, server = server)

