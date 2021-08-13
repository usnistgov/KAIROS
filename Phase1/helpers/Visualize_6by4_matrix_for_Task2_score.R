## load library
library(tidyverse)
library(readxl)
library(tidyr)
library(ggplot2)
cbPalette <- c("#D55E00","#009E73", "#56B4E9", "#CC79A7", "#F0E442","#999999", "#E69F00", "#0072B2")
cbbPalette <- c("#000000", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")



####
## Task 2 Recall of graph G Events
file_name <- "../Task2_score_analysis/Schema_avg/schema_avg_ev_ta1_gev_all.xlsx"
stdfile_name <- "../Task2_score_analysis/Schema_avg/schema_std_ev_ta1_gev_all.xlsx"
input_data <- read_excel(file_name)
input_std <- read_excel(stdfile_name)
mydata <- merge(input_data, input_std,by=c("TA1","TA2"))
mydata$TA1 = toupper(mydata$TA1)
mydata$TA2 = toupper(mydata$TA2)

ggplot(mydata, aes(x=TA1, y=recall_ta1_gev_all.x, fill=TA2)) + 
  geom_bar(stat="identity", position=position_dodge(), alpha=0.8) +
  geom_errorbar( aes(ymin=recall_ta1_gev_all.x-recall_ta1_gev_all.y, ymax=recall_ta1_gev_all.x+recall_ta1_gev_all.y), width=0.6, colour="black", alpha=0.7, size=0.4, position=position_dodge(.9)) +

  theme(plot.title = element_text(hjust = 0.5)) +
  ylim(-0.077,1.0) +
  scale_fill_manual(values=cbPalette) +
  ylab("Recall of graph G Events") +
  theme(axis.text.x = element_text(size = 15, hjust = .5, vjust = .5),
        axis.text.y = element_text(size = 15, hjust = .5, vjust = .5),
        axis.title.x = element_text(size = 20, hjust = .5, vjust = .5),
        axis.title.y = element_text(size = 20, hjust = .5, vjust = .5),
        legend.title = element_text(size = 20, hjust = .5, vjust = .5),
        legend.text = element_text(size = 15, hjust = .5, vjust = .5))
plot_name <- paste0("../Figures/task2_recall_ta1_gev_all.png")
ggsave(plot_name)





####
## load data
file_name <- "./Schema_mean/schema_mean_ev_ta1_aev_st_all.xlsx"
input_data <- read_excel(file_name)

## draw plots
ggplot(input_data, aes(x=TA1, y=recall_ta1_aev_st_all, fill=TA2)) + 
  geom_bar(stat="identity", position=position_dodge()) +
  theme(plot.title = element_text(hjust = 0.5)) +
  ylim(0,0.8) +
  scale_fill_manual(values=cbPalette) + 
plot_name <- paste0("../Figures/schema_mean_ev_ta1_aev_st_all.png")
ggsave(plot_name)

####
## load data
file_name <- "../Task2_score_analysis/Schema_mean/schema_mean_ev_ta1_aev_sst_all.xlsx"
input_data <- read_excel(file_name)

## draw plots
ggplot(input_data, aes(x=TA1, y=recall_ta1_aev_sst_all, fill=TA2)) + 
  geom_bar(stat="identity", position=position_dodge()) +
  theme(plot.title = element_text(hjust = 0.5))
plot_name <- paste0("../Figures/schema_mean_ev_ta1_aev_sst_all.png")
ggsave(plot_name)

####
## load data
file_name <- "../Task2_score_analysis/Schema_mean/schema_mean_ev_ta1_aev_sst_crt.xlsx"
input_data <- read_excel(file_name)

## draw plots
ggplot(input_data, aes(x=TA1, y=recall_ta1_aev_sst_crt, fill=TA2)) + 
  geom_bar(stat="identity", position=position_dodge()) +
  theme(plot.title = element_text(hjust = 0.5))
plot_name <- paste0("../Figures/schema_mean_ev_ta1_aev_sst_crt.png")
ggsave(plot_name)

####
## load data
file_name <- "../Task2_score_analysis/Schema_mean/schema_mean_ev_ta1_aev_st_all.xlsx"
input_data <- read_excel(file_name)

## draw plots
ggplot(input_data, aes(x=TA1, y=recall_ta1_aev_st_all, fill=TA2)) + 
  geom_bar(stat="identity", position=position_dodge()) +
  theme(plot.title = element_text(hjust = 0.5))
plot_name <- paste0("../Figures/schema_mean_ev_ta1_aev_st_all.png")
ggsave(plot_name)

####
## load data
file_name <- "../Task2_score_analysis/Schema_mean/schema_mean_ev_ta1_aev_st_crt.xlsx"
input_data <- read_excel(file_name)

## draw plots
ggplot(input_data, aes(x=TA1, y=recall_ta1_aev_st_crt, fill=TA2)) + 
  geom_bar(stat="identity", position=position_dodge()) +
  theme(plot.title = element_text(hjust = 0.5))
plot_name <- paste0("../Figures/schema_mean_ev_ta1_aev_st_crt.png")
ggsave(plot_name)

####
## load data
file_name <- "../Task2_score_analysis/Schema_mean/schema_mean_order.xlsx"
input_data <- read_excel(file_name)

## draw plots
ggplot(input_data, aes(x=TA1, y=recall, fill=TA2)) + 
  geom_bar(stat="identity", position=position_dodge()) +
  theme(plot.title = element_text(hjust = 0.5))
plot_name <- paste0("../Figures/schema_mean_order.png")
ggsave(plot_name)

####
## load data
file_name <- "../Task2_score_analysis/Schema_mean/schema_mean_rel.xlsx"
input_data <- read_excel(file_name)

## draw plots
ggplot(input_data, aes(x=TA1, y=recall, fill=TA2)) + 
  geom_bar(stat="identity", position=position_dodge()) +
  theme(plot.title = element_text(hjust = 0.5))
plot_name <- paste0("../Figures/schema_mean_rel.png")
ggsave(plot_name)

