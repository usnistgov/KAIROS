## load library
library(tidyverse)
library(readxl)
library(tidyr)
library(ggplot2)
cbPalette <- c("#D55E00","#009E73", "#56B4E9", "#CC79A7", "#F0E442","#999999", "#E69F00", "#0072B2")
cbbPalette <- c("#000000", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")



####
## Task 1 Event Precision
file_name <- "../Task1_score_analysis/Schema_avg/schema_avg_ev_score_precision.xlsx"
stdfile_name <- "../Task1_score_analysis/Schema_avg/schema_std_ev_score_precision.xlsx"
input_data <- read_excel(file_name)
input_std <- read_excel(stdfile_name)
mydata <- merge(input_data, input_std,by=c("TA1","TA2"))
mydata$TA1 = toupper(mydata$TA1)
mydata$TA2 = toupper(mydata$TA2)

ggplot(mydata, aes(x=TA2, y=precision.x, fill=TA1)) + 
  geom_bar(stat="identity", position=position_dodge(), alpha=0.8) +
  geom_errorbar( aes(ymin=precision.x-precision.y, ymax=precision.x+precision.y), width=0.6, colour="black", alpha=0.7, size=0.4, position=position_dodge(.9)) +

  theme(plot.title = element_text(hjust = 0.5)) +
  ylim(-0.077,1.0) +
  scale_fill_manual(values=cbPalette) +
  ylab("Precision of Events") +
  theme(axis.text.x = element_text(size = 15, hjust = .5, vjust = .5),
        axis.text.y = element_text(size = 15, hjust = .5, vjust = .5),
        axis.title.x = element_text(size = 20, hjust = .5, vjust = .5),
        axis.title.y = element_text(size = 20, hjust = .5, vjust = .5),
        legend.title = element_text(size = 20, hjust = .5, vjust = .5),
        legend.text = element_text(size = 15, hjust = .5, vjust = .5))
plot_name <- paste0("../Figures/task1_event_precision.png")
ggsave(plot_name)





####
## Task 1 Event Precision without extra-relevant
file_name <- "../Task1_score_analysis/Schema_avg/schema_avg_ev_score_precision_woer.xlsx"
stdfile_name <- "../Task1_score_analysis/Schema_avg/schema_std_ev_score_precision_woer.xlsx"
input_data <- read_excel(file_name)
input_std <- read_excel(stdfile_name)
mydata <- merge(input_data, input_std,by=c("TA1","TA2"))
mydata$TA1 = toupper(mydata$TA1)
mydata$TA2 = toupper(mydata$TA2)

ggplot(mydata, aes(x=TA2, y=precision_woer.x, fill=TA1)) + 
  geom_bar(stat="identity", position=position_dodge(), alpha=0.8) +
  geom_errorbar( aes(ymin=precision_woer.x-precision_woer.y, ymax=precision_woer.x+precision_woer.y), width=0.6, colour="black", alpha=0.7, size=0.4, position=position_dodge(.9)) +
  
  theme(plot.title = element_text(hjust = 0.5)) +
  ylim(-0.077,1.0) +
  scale_fill_manual(values=cbPalette) +
  ylab("Event Precision without extra-relevant") +
  theme(axis.text.x = element_text(size = 15, hjust = .5, vjust = .5),
        axis.text.y = element_text(size = 15, hjust = .5, vjust = .5),
        axis.title.x = element_text(size = 20, hjust = .5, vjust = .5),
        axis.title.y = element_text(size = 20, hjust = .5, vjust = .5),
        legend.title = element_text(size = 20, hjust = .5, vjust = .5),
        legend.text = element_text(size = 15, hjust = .5, vjust = .5))
plot_name <- paste0("../Figures/task1_event_precision_woer.png")
ggsave(plot_name)


####
## Task 1 Event Precision@20
file_name <- "../Task1_score_analysis/Schema_avg/schema_avg_ev_score_precision_at_top_20.xlsx"
stdfile_name <- "../Task1_score_analysis/Schema_avg/schema_std_ev_score_precision_at_top_20.xlsx"
input_data <- read_excel(file_name)
input_std <- read_excel(stdfile_name)
mydata <- merge(input_data, input_std,by=c("TA1","TA2"))
mydata$TA1 = toupper(mydata$TA1)
mydata$TA2 = toupper(mydata$TA2)

ggplot(mydata, aes(x=TA2, y=precision_at_top_20.x, fill=TA1)) + 
  geom_bar(stat="identity", position=position_dodge(), alpha=0.8) +
  geom_errorbar( aes(ymin=precision_at_top_20.x-precision_at_top_20.y, ymax=precision_at_top_20.x+precision_at_top_20.y), width=0.6, colour="black", alpha=0.7, size=0.4, position=position_dodge(.9)) +
  
  theme(plot.title = element_text(hjust = 0.5)) +
  ylim(-0.077,1.0) +
  scale_fill_manual(values=cbPalette) +
  ylab("Event Precision@20") +
  theme(axis.text.x = element_text(size = 15, hjust = .5, vjust = .5),
        axis.text.y = element_text(size = 15, hjust = .5, vjust = .5),
        axis.title.x = element_text(size = 20, hjust = .5, vjust = .5),
        axis.title.y = element_text(size = 20, hjust = .5, vjust = .5),
        legend.title = element_text(size = 20, hjust = .5, vjust = .5),
        legend.text = element_text(size = 15, hjust = .5, vjust = .5))
plot_name <- paste0("../Figures/task1_event_precision_at_top_20.png")
ggsave(plot_name)


####
## Task 1 Event Precision@20 without extra-relevant
file_name <- "../Task1_score_analysis/Schema_avg/schema_avg_ev_score_precision_at_top_20_woer.xlsx"
stdfile_name <- "../Task1_score_analysis/Schema_avg/schema_std_ev_score_precision_at_top_20_woer.xlsx"
input_data <- read_excel(file_name)
input_std <- read_excel(stdfile_name)
mydata <- merge(input_data, input_std,by=c("TA1","TA2"))
mydata$TA1 = toupper(mydata$TA1)
mydata$TA2 = toupper(mydata$TA2)

ggplot(mydata, aes(x=TA2, y=precision_at_top_20_woer.x, fill=TA1)) + 
  geom_bar(stat="identity", position=position_dodge(), alpha=0.8) +
  geom_errorbar( aes(ymin=precision_at_top_20_woer.x-precision_at_top_20_woer.y, ymax=precision_at_top_20_woer.x+precision_at_top_20_woer.y), width=0.6, colour="black", alpha=0.7, size=0.4, position=position_dodge(.9)) +
  
  theme(plot.title = element_text(hjust = 0.5)) +
  ylim(-0.077,1.0) +
  scale_fill_manual(values=cbPalette) +
  ylab("Event Precision@20 without extra-relevant") +
  theme(axis.text.x = element_text(size = 15, hjust = .5, vjust = .5),
        axis.text.y = element_text(size = 15, hjust = .5, vjust = .5),
        axis.title.x = element_text(size = 20, hjust = .5, vjust = .5),
        axis.title.y = element_text(size = 20, hjust = .5, vjust = .5),
        legend.title = element_text(size = 20, hjust = .5, vjust = .5),
        legend.text = element_text(size = 15, hjust = .5, vjust = .5))
plot_name <- paste0("../Figures/task1_event_precision_at_top_20_woer.png")
ggsave(plot_name)
