

library(shiny)
library(shinydashboard)
library(shinythemes)
library(plotly)
library(dplyr)
library(ggplot2)
library(markdown)
library(rjson)
library(RCurl)



# Data Import
  
   # Aggregate Income Statement Data
     aggregate.is.text <- getURL("https://raw.githubusercontent.com/Nnavarr/Post-Acquisition-Analysis/master/Aggregate_IS_Data.csv?token=AiT95b3R2JF_arMzwrMm1JieduB064cBks5bdfwOwA%3D%3D")
      aggregate.is.df <- read.csv(text = aggregate.is.text)



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
  
     
      plot_ly(aggregate.is.df, x = ~Date, y = ~Value, mode = 'lines')
     
   })
} 
  

# Run the application 
shinyApp(ui = ui, server = server)

