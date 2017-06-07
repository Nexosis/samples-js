class ResultsController < ApplicationController

  def index
    params.require(:sessionId)
    client = ApiClient.new(Rails.application.secrets.api_key)
    
    @sessionResult = client.get_session_results(params[:sessionId]);
    #TODO: cannot determine pages from API - need dataset information to be more specific
    if @sessionResult.session.type == "forecast"
      firstPred = Date.parse(@sessionResult.data[0]["timestamp"])
      lastObs =  firstPred - 1
      firstObs = firstPred - 30
      @observations = client.get_dataset(@sessionResult.session.dataSetName, 0, 30, firstObs, lastObs)
    else
      firstObs = Date.parse(@sessionResult.session.startDate) - 15
      lastObs = Date.parse(@sessionResult.session.endDate) + 15
      totalEventDays = (Date.parse(@sessionResult.session.endDate) - Date.parse(@sessionResult.session.startDate)).to_i
      @observations = client.get_dataset(@sessionResult.session.dataSetName, 0, totalEventDays + 30, firstObs, lastObs)
    end
  end

  def file
    params.require(:sessionId)
    client = ApiClient.new(Rails.application.secrets.api_key)
    @sessionResult = client.get_session_results(params[:sessionId], true);
    filename = "#{params[:sessionId]}.csv"
    response.headers['Content-Disposition'] = "attachment;filename=#{filename}"
    render inline: @sessionResult, content_type: 'text/csv'
  end

  def champion
    params.require(:dataset_name)
    params.require(:target_column)
    client = ApiClient.new(Rails.application.secrets.api_key)
    @champion = client.get_forecast_model(params["dataset_name"], params["target_column"])
    render
  end
end
