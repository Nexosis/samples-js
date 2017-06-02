class ResultsController < ApplicationController
  def index
    params.require(:sessionId)
    client = ApiClient.new(Rails.application.secrets.api_key)
    
    @sessionResult = client.get_session_results(params[:sessionId]);
    #TODO: cannot determine pages from API - need dataset information to be more specific
    firstPred = Date.parse(@sessionResult.data[0]["timestamp"])
    lastObs =  firstPred - 1
    firstObs = firstPred - 30
    @observations = client.get_dataset(@sessionResult.session.dataSetName, 0, 30, firstObs, lastObs)
  end

  def champion
    params.require(:dataset_name)
    params.require(:target_column)
    client = ApiClient.new(Rails.application.secrets.api_key)
    @champion = client.get_forecast_model(params["dataset_name"], params["target_column"])
    render
  end
end
