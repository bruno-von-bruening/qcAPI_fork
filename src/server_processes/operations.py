from . import *
from util.sql_util import get_primary_key, get_primary_key_name

@validate_call
def parse_dict(filters_plain:List[str]):
    try:
        new_filters={}
        for x in filters_plain:
            sep='--'
            ar=x.split(sep)
            assert len(ar)==2, f"Expected separator \'{sep}\' in filters option but got {x}"
            new_filters.update({ar[0]:ar[1]})
        return new_filters
    except Exception as ex: my_exception(f"Could not interprete as dictionary:\n{filters_plain}",ex)

@validate_call
def get_all_ids(session, object):
    return session.exec(select(get_primary_key(object))).all()

def operation_functions(app, SessionDep):
    @app.post("/reset/{prop}")
    async def reset(
        prop: str,
        session: SessionDep,
        ids: List[str|int] = Query(None),
        filters: List[str] = Query([]),
        force: bool=False,
        clone: bool=False,
        help:   bool=False,
    ):
        """
        :param bool force: (default=True)
        """

        # print help
        if help:
            raise HTTPException(HTTPStatus.IM_A_TEAPOT, reset.__doc__)
        # Prepare the obtained objects
        try:
            the_object=get_object_for_tag(prop)
            if ids is None:
                ids=get_all_ids(session,the_object)
            filters=parse_dict(filters)
        except Exception as ex: raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR,
                f"Could not preprocess data for {reset}: {analyse_exception(ex)}")

        try:
            #
            status_converged=1
            query=select(the_object).where(get_primary_key(the_object).in_(ids)).where(the_object.converged!=status_converged)
            for k,v in filters.items():
                try:
                    attr=getattr(the_object,k)
                except Exception as ex:
                    raise Exception(f"Object \'{the_object.__name__}\' has no attribute \'{k}\' (vailable keys={[x for x in list(the_object.__dict__.keys()) if not x.startswith('_') and not x.endswith('_')]}")
                query=query.where(attr==v)
            results=session.exec(query).all()

            # Reset the convergence status
            if force:
                status_pending=-1
                for r in results:
                    r.converged=status_pending
                    # If clone is active clone the row by letting a new id get assigned
                    if clone:
                        setattr(r,get_primary_key_name(r),None)
                    session.add(r)
                    session.add(r)
                try:
                    session.commit()
                except Exception as ex:
                    if clone:
                        addendum=f" (This could happen because you tried to clone but the object is assigned an id by its data)"
                    else:
                        addendum=''
                    raise Exception(f"Could not update database table of type {the_object.__name__}{addendum}: {ex}")
            else:
                raise Exception(f"You are about to reset the status of {len(results)} files are you sure about that (could use the option clone instead for the moment and delete after)")
            
            return {'message':f"Deleted {len(results)} of type {the_object.__name__} (with filter={filters})"}
        except Exception as ex: raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR,
                f"Could not process delete request: {analyse_exception(ex)}")        


    @app.post("/delete/{prop}")
    async def delete(
        prop: str,
        session: SessionDep,
    ):
        try:
            the_object=object_mapper[prop]
            entries= session.exec( select(the_object) ).all()
            for entry in entries:
                session.delete(entry)
            session.commit()
        except Exception as ex:
            raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR, f"Could not execute delete for {prop}: {analyse_exception(ex)}")

    @app.post("/clean_pending/{prop}")
    async def clean_double(
        prop: str,
        session: SessionDep,
        force:bool=False,
    ):
        try:
            the_object=object_mapper[prop]
            pending_status_key=-1
            the_pending=session.exec(
                select(the_object)
                .where(the_object.converged==pending_status_key)
            ).all()
            [ session.delete(x) for x in the_pending ]
            session.commit()
            message=f"Deleted {len(the_pending)} objects of type {the_object.__name__}"
            return {'message':message}
        except Exception as ex: raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR,f"Could not execute {clean_double}: {analyse_exception(ex)}")

    @app.post("/clean_double/{prop}")
    async def clean_double(
        prop: str,
        session: SessionDep,
        force:bool=False,
    ):
        try:
            the_object=object_mapper[prop]
            if the_object==IsoDens_Surface:
                #entries = [ x for x in
                #           session.exec( select(the_object.id, the_object.wave_function_id,the_object.iso_density, the_object.spacing, the_object.converged)).all()]
                entries = [ x for x in
                           session.exec( select(the_object.id, the_object.wave_function_id,the_object.iso_density, the_object.spacing, the_object.converged)).all()]
                
                # see all entries where iso_density and spacing is the same
                iso_densities=list(set([ e[2] for e in entries]))
                spacing=list(set([ e[3] for e in entries]))
                
                pair_dic=dict([ (iso, dict([  (x,[]) for x in spacing  ])) for iso in iso_densities])

                data=[ [e[2],e[3]] for e in entries]
                for i, (iso_d,spac) in enumerate(data):
                    pair_dic[iso_d][spac].append(i)

                for isod in iso_densities:
                    for spac in spacing:
                        try:
                            ids=[ (entries[ind][0],entries[ind][1], entries[ind][4]) for ind in pair_dic[isod][spac]]
                        except:
                            raise Exception(f"{isod} {spac} {dic.keys()} {iso_densities}")
                        dic={}
                        for isod_id, wfn_id, converged in ids:
                            if not wfn_id in dic.keys():
                                dic.update({wfn_id:[]})
                            dic[wfn_id].append( (isod_id, converged) )
                        for wfn_id, row in dic.items():
                            converged=[ x[0] for x in row if x[1]==1]
                            not_converged=[ x[0] for x in row if x[1]!=1]
                            if len(converged)==0:
                                pass
                            elif len(converged)>1:
                                if force:
                                    # remove all except the lowest id
                                    delete_ids=sorted(converged)[1:]
                                    assert len(delete_ids)+1==len(converged),f"Problem in deleting, check failed."
                                    [ 
                                        session.delete( session.get(the_object,isod_id)) 
                                        for isod_id  in delete_ids
                                    ]
                                    session.commit()
                                else:    
                                    raise Exception(f"Found more than one converged item (remove all except smallest id)")
            elif the_object==RHO_ESP_Map:
                entries=session.exec( select(the_object.id, the_object.surface_id, the_object.converged) ).all()
                dic={}
                for id, surf_id, conv in entries:
                    if not surf_id in dic.keys():
                        dic.update({surf_id:[]})
                    dic[surf_id].append( (id, conv))
                for surf_id, vals in dic.items():
                    if len(vals)>1:
                        converged=[ x[0] for x in vals if x[1]==1]
                        pending=[ x[0] for x in vals if x[1]==-1]
                        if len(converged)>1:
                            if force:
                                delete_ids=sorted(converged)[1:]
                                assert len(delete_ids)+1==len(converged),f"Problem in deleting, check failed."
                                [ 
                                    session.delete( session.get(the_object,isod_id)) 
                                    for isod_id  in delete_ids
                                ]
                                session.commit()
                            else:    
                                raise Exception(f"Found more than one converged item (remove all except smallest id)")
                        if len(converged)>0:
                            [ session.delete(session.get(the_object,the_id)) for the_id in pending ]
                session.commit()
            elif the_object==DMP_vs_RHO_ESP_Map:
                entries=session.exec( select(the_object.id, the_object.dmp_map_id, the_object.rho_map_id, the_object.converged) ).all()
                dic={}
                for id, dmp_id, rho_id, conv in entries:
                    if not dmp_id in dic.keys():
                        dic.update({dmp_id:{}})
                    if not rho_id in dic[dmp_id].keys():
                        dic[dmp_id].update({rho_id:[]})

                    dic[dmp_id][rho_id].append( (id, conv))
                for dmp_id, inner_dic in dic.items():
                    for rho_id, vals in inner_dic.items():
                        if len(vals)>1:
                            converged=[ x[0] for x in vals if x[1]==1]
                            pending=[ x[0] for x in vals if x[1]==-1]
                            assert len(converged)<2
                            if len(converged)>0:
                                [ session.delete(session.get(the_object,the_id)) for the_id in pending ]
                session.commit()


                                
                            
                                
                            



                


        except Exception as ex: raise HTTPException(HTTPStatus.INTERNAL_SERVER_ERROR,
            f"Couldn noe execute {clean_double}: {analyse_exception(ex)}")

