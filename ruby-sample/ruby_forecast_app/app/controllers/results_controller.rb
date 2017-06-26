require 'nexosis_api'

class ResultsController < ApplicationController

  def index
    params.require(:sessionId)
    
    @sessionResult = @api_client.get_session_results(params[:sessionId]);
    #TODO: cannot determine pages from API - need dataset information to be more specific
    if @sessionResult.type == "forecast"
      firstPred = Date.parse(@sessionResult.data[0]["timestamp"])
      lastObs =  firstPred - 1
      firstObs = firstPred - 30
      @observations = @api_client.get_dataset(@sessionResult.dataSetName, 0, 30, {:start_date => firstObs, :end_date => lastObs})
    else
      firstObs = Date.parse(@sessionResult.startDate) - 15
      lastObs = Date.parse(@sessionResult.endDate) + 15
      totalEventDays = (Date.parse(@sessionResult.endDate) - Date.parse(@sessionResult.startDate)).to_i
      @observations = @api_client.get_dataset(@sessionResult.dataSetName, 0, totalEventDays + 30, {:start_date => firstObs, :end_date => lastObs})
    end
  end

  def file
    params.require(:sessionId)
    client = NexosisApi.client(Rails.application.secrets.api_key)
    @sessionResult = client.get_session_results(params[:sessionId], true);
    filename = "#{params[:sessionId]}.csv"
    response.headers['Content-Disposition'] = "attachment;filename=#{filename}"
    render inline: @sessionResult, content_type: 'text/csv'
  end
end
