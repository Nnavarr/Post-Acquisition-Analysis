

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
  header <- dashboardHeader(
    title = "U-Haul Dashboard"
    #valueBoxOutput("quarter")
    #valueBoxOutput("fy")
  )
  
  
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
      
      valueBoxOutput("quarter")
      
      # Value Boxes
       # infoBoxOutput("quarter")
      
    ),
      
      
      
      # Main Graph Control Drop Downs
        fixedRow(
          column(1, selectInput(inputId = "Group", label = "Acq Group", choices = aggregate.is.df$Group, selected = "FY15 Q4")),
          column(2, selectInput(inputId = "Line", label = "Line Item", choices = aggregate.is.df$Line.Item, selected = 'Op_NOI'))
        ),
      
      
      
      # Row 1 (Line Item Graph)
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
  


# Define UI for application (Dashboard) ----
ui <- dashboardPage(header = header,
                    sidebar = sidebar,
                    body = body)
   
 
        
        
#------------------------------------------------------------------------------        

# Server Inputs ----        
        
# Define server logic 
server <- function(input, output) {
  
  
  
  # Observe Line Item Input for updating the graph ----
    observeEvent(input$Group, {
      
      observeEvent(input$Line, {
      
      # Create Convenience data.frame for line item graph
        plot.lineitem <- data.frame(aggregate.is.df %>%
                                      dplyr::filter(Group == input$Group & Line.Item == input$Line))
        
      # Create Convenience data.frame for Value Boxes
        boxes.df <- data.frame(aggregate.is.df %>%
                                 dplyr::filter(Group == input$Group & Line.Item == input$Line) %>%
                                 dplyr::group_by(Line.Item, FY, Quarter, Group) %>%
                                 dplyr::summarise(Value = sum(Value))
                                 )
        
          # Calculate Y/Y Quarter Comparison 
            quarter.matrix <- as.matrix(boxes.df$Value)
             df.row <- nrow(quarter.matrix)
              yoy.q <- diff(quarter.matrix, lag = 4)
               quarter.yoy <- (yoy.q[nrow(yoy.q)] /  quarter.test[nrow(quarter.test), 5]) * 100
               

  
  # Render Value Boxes ----
  output$quarter <- renderValueBox({
   valueBox(
     
     quarter.yoy$Value,
     "Year over Year",
     icon = icon("caret-up")
            
   )
    })
              
        
  # Render Line Item Plot      
   output$lineitem <- renderPlotly({
  
      plot_ly(plot.lineitem, x = ~Date, y = ~Value, mode = 'lines')
     
     })
   
  })
      
  })
  
}

        

# Run the application 
shinyApp(ui = ui, server = server)




