

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
        aggregate.is.df$Date = as.Date(aggregate.is.df$Date)
           
        
        
        
      
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
      
      
      # Line Item Selection
        fixedRow(
          column(1, selectInput(inputId = "Group", label = "Acquisition Group", choices = aggregate.is.df$Group, selected = "FY15 Q4")),
          column(2, selectInput(inputId = "LineItem", label = "Line Item", choices = aggregate.is.df$Line.Item, selected = 'Op_NOI'))
        ),
      
      
      
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
   
 
        
        
        
        
        
#------------------------------------------------------------------------------        
        


# Define server logic required to draw a histogram
server <- function(input, output) {
  
  # Observe Line Item Input for updating the graph
    observeEvent(input$Group, {
      
      # Create Convenience data.frame for line item graph
        plot.lineitem <- data.frame(aggregate.is.df %>%
                                      filter(Group == input$Group))


        
  # Render Line Item Plot      
   output$lineitem <- renderPlotly({
  
      plot_ly(plot.lineitem, x = ~Date, y = ~Value, mode = 'lines')
     
   })
})   
   
}

        
# Run the application 
shinyApp(ui = ui, server = server)




